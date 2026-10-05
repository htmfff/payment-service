from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from app.core import constants
from app.core.enums import PaymentStatus
from app.core.exceptions import PaymentDeclinedError, PaymentNotFoundError
from app.core.time import utcnow
from app.db.models import Payment
from app.db.repositories import PaymentRepository
from app.db.session import Database, transaction
from app.logging import get_logger
from app.messaging.events import PaymentCreatedEvent
from app.services.gateway import PaymentGateway
from app.services.webhooks import WebhookDispatcher, webhook_payload_from_payment

logger = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class ChargeOutcome:
    status: PaymentStatus
    processed_at: datetime
    gateway_reference: str | None
    failure_reason: str | None


class PaymentProcessor:
    """Processes one `payment.created` event end to end.

    The handler is idempotent by construction: the transition to a terminal state is guarded by
    `status = pending`, so a redelivered message can never charge or notify twice.
    """

    def __init__(
        self,
        database: Database,
        gateway: PaymentGateway,
        webhooks: WebhookDispatcher,
    ) -> None:
        self._database = database
        self._gateway = gateway
        self._webhooks = webhooks

    async def process(self, event: PaymentCreatedEvent) -> None:
        payment = await self._claim(event.payment_id)
        if payment is None:
            raise PaymentNotFoundError(event.payment_id)

        if payment.status is not PaymentStatus.PENDING:
            logger.info(
                "payment already settled, nothing to do",
                extra={"payment_id": str(payment.id), "status": payment.status.value},
            )
            return

        if not await self._settle(payment.id, await self._charge(payment)):
            logger.info(
                "payment settled concurrently, skipping notification",
                extra={"payment_id": str(payment.id)},
            )
            return

        settled = await self._reload(payment.id)
        if settled is None:
            raise PaymentNotFoundError(payment.id)

        await self._notify(settled)

    async def _claim(self, payment_id: UUID) -> Payment | None:
        async with self._database.session() as session:
            async with transaction(session):
                repository = PaymentRepository(session)
                await repository.increment_processing_attempts(payment_id)
                return await repository.get(payment_id)

    async def _reload(self, payment_id: UUID) -> Payment | None:
        async with self._database.session() as session:
            return await PaymentRepository(session).get(payment_id)

    async def _charge(self, payment: Payment) -> ChargeOutcome:
        try:
            result = await self._gateway.charge(
                payment_id=payment.id,
                amount=payment.amount,
                currency=payment.currency,
                idempotency_key=payment.idempotency_key,
            )
        except PaymentDeclinedError as declined:
            logger.info(
                "gateway declined the charge",
                extra={"payment_id": str(payment.id), "reason": declined.message},
            )
            return ChargeOutcome(
                status=PaymentStatus.FAILED,
                processed_at=utcnow(),
                gateway_reference=None,
                failure_reason=declined.message,
            )

        return ChargeOutcome(
            status=PaymentStatus.SUCCEEDED,
            processed_at=result.processed_at,
            gateway_reference=result.reference,
            failure_reason=None,
        )

    async def _settle(self, payment_id: UUID, outcome: ChargeOutcome) -> bool:
        async with self._database.session() as session:
            async with transaction(session):
                return await PaymentRepository(session).mark_processed(
                    payment_id,
                    status=outcome.status,
                    gateway_reference=outcome.gateway_reference,
                    failure_reason=outcome.failure_reason,
                    processed_at=outcome.processed_at,
                )

    async def _notify(self, payment: Payment) -> None:
        delivery = await self._webhooks.deliver(
            payment.webhook_url,
            webhook_payload_from_payment(payment),
        )
        if delivery.delivered:
            return

        logger.error(
            "webhook delivery attempts exhausted",
            extra={
                "payment_id": str(payment.id),
                "attempts": delivery.attempts,
                "error": delivery.last_error,
            },
        )
        async with self._database.session() as session:
            async with transaction(session):
                await PaymentRepository(session).record_webhook_attempt(
                    payment.id,
                    attempts=delivery.attempts,
                    last_error=delivery.last_error,
                )


def read_attempt(headers: dict[str, object] | None) -> int:
    """Current delivery attempt, taken from the message headers we set when scheduling a retry."""
    value = (headers or {}).get(constants.ATTEMPT_HEADER, constants.DEFAULT_ATTEMPT)
    if not isinstance(value, (int, str)) or isinstance(value, bool):
        return constants.DEFAULT_ATTEMPT
    try:
        return max(constants.DEFAULT_ATTEMPT, int(value))
    except ValueError:
        return constants.DEFAULT_ATTEMPT