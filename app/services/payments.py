from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from app.core import constants
from app.core.enums import Currency, PaymentStatus
from app.core.exceptions import IdempotencyKeyConflictError
from app.core.hashing import fingerprint
from app.core.time import utcnow
from app.db.models import Payment
from app.db.repositories import OutboxRepository, PaymentRepository
from app.db.session import transaction
from app.logging_config import get_logger
from app.messaging.events import PaymentCreatedEvent

logger = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class CreatePaymentCommand:
    amount: Decimal
    currency: Currency
    description: str
    webhook_url: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def request_fingerprint(self) -> str:
        return fingerprint(
            {
                "amount": self.amount,
                "currency": self.currency,
                "description": self.description,
                "metadata": self.metadata,
                "webhook_url": self.webhook_url,
            },
        )


@dataclass(frozen=True, slots=True)
class CreatePaymentOutcome:
    payment: Payment
    replayed: bool


class PaymentService:
    """Creates payments and, in the same transaction, records the event that will be published.

    Nothing is written to RabbitMQ here. The outbox row is the hand-off point, which removes the
    classic "database committed but the broker never got the message" failure.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._payments = PaymentRepository(session)
        self._outbox = OutboxRepository(session)

    async def create(
        self,
        command: CreatePaymentCommand,
        *,
        idempotency_key: str,
    ) -> CreatePaymentOutcome:
        request_fingerprint = command.request_fingerprint()

        async with transaction(self._session):
            created = await self._payments.insert_ignoring_duplicates(
                self._new_payment(command, idempotency_key, request_fingerprint),
            )
            if created is None:
                replayed = await self._resolve_replay(idempotency_key, request_fingerprint)
                logger.info(
                    "idempotent replay served",
                    extra={"payment_id": str(replayed.id), "idempotency_key": idempotency_key},
                )
                return CreatePaymentOutcome(payment=replayed, replayed=True)

            event = self._new_event(created)
            await self._outbox.append(
                aggregate_id=created.id,
                event_type=event.event_type,
                routing_key=constants.PAYMENT_CREATED_ROUTING_KEY,
                payload=event.model_dump(mode="json"),
            )
            logger.info(
                "payment created",
                extra={
                    "payment_id": str(created.id),
                    "amount": str(created.amount),
                    "currency": created.currency.value,
                    "event_id": str(event.event_id),
                },
            )
            return CreatePaymentOutcome(payment=created, replayed=False)

    async def get(self, payment_id: UUID) -> Payment | None:
        return await self._payments.get(payment_id)

    def _new_payment(
        self,
        command: CreatePaymentCommand,
        idempotency_key: str,
        request_fingerprint: str,
    ) -> Payment:
        return Payment(
            id=uuid4(),
            idempotency_key=idempotency_key,
            request_fingerprint=request_fingerprint,
            amount=command.amount,
            currency=command.currency,
            description=command.description,
            metadata_=dict(command.metadata),
            webhook_url=command.webhook_url,
            status=PaymentStatus.PENDING,
        )

    async def _resolve_replay(
        self,
        idempotency_key: str,
        request_fingerprint: str,
    ) -> Payment:
        existing = await self._payments.get_by_idempotency_key(idempotency_key)
        if existing is None:
            raise IdempotencyKeyConflictError(
                "Idempotency-Key is already in use but the original payment is not visible yet",
            )
        if existing.request_fingerprint != request_fingerprint:
            raise IdempotencyKeyConflictError(
                "Idempotency-Key was already used with a different request body",
            )
        return existing

    @staticmethod
    def _new_event(payment: Payment) -> PaymentCreatedEvent:
        return PaymentCreatedEvent(
            event_id=uuid4(),
            occurred_at=utcnow(),
            payment_id=payment.id,
            idempotency_key=payment.idempotency_key,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            metadata=dict(payment.metadata_),
            webhook_url=payment.webhook_url,
        )
