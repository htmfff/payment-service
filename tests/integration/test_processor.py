from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import httpx
import pytest

from app.core.enums import Currency, PaymentStatus
from app.core.exceptions import PaymentDeclinedError
from app.core.time import utcnow
from app.db.models import Payment
from app.db.session import Database, transaction
from app.messaging.events import PaymentCreatedEvent
from app.services.gateway import ChargeResult, PaymentGateway
from app.services.webhooks import PaymentWebhookPayload, WebhookDispatcher
from app.worker.handlers import PaymentProcessor

pytestmark = pytest.mark.integration

_WEBHOOK_URL = "http://localhost:8080/hooks/payments"
_SECRET = "test-webhook-secret"


class StubGateway(PaymentGateway):
    def __init__(self, *, approved: bool = True, raises: Exception | None = None) -> None:
        self.approved = approved
        self.raises = raises
        self.calls: list[UUID] = []

    async def charge(
        self,
        *,
        payment_id: UUID,
        amount: Decimal,
        currency: Currency,
        idempotency_key: str,
    ) -> ChargeResult:
        self.calls.append(payment_id)
        if self.raises is not None:
            raise self.raises
        if not self.approved:
            raise PaymentDeclinedError("card was declined")
        return ChargeResult(reference="ch_stub", processed_at=utcnow())


def _dispatcher(
    responses: Callable[[httpx.Request], httpx.Response],
) -> tuple[WebhookDispatcher, list[httpx.Request]]:
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return responses(request)

    async def no_sleep(_: float) -> None:
        return None

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    dispatcher = WebhookDispatcher(
        client,
        signing_secret=_SECRET,
        max_attempts=3,
        timeout_seconds=1.0,
        backoff_base_seconds=0,
        sleep=no_sleep,
    )
    return dispatcher, captured


def _processor(
    database: Database,
    gateway: PaymentGateway,
    dispatcher: WebhookDispatcher,
) -> PaymentProcessor:
    return PaymentProcessor(database, gateway, dispatcher)


async def _seed(database: Database, idempotency_key: str) -> Payment:
    async with database.session() as session, transaction(session):
        payment = Payment(
            idempotency_key=idempotency_key,
            request_fingerprint="a" * 64,
            amount=Decimal("1490.50"),
            currency=Currency.RUB,
            description="Processor test",
            metadata_={"order_id": "A-1"},
            webhook_url=_WEBHOOK_URL,
            status=PaymentStatus.PENDING,
        )
        session.add(payment)
        await session.flush()
        return payment


def _event(payment: Payment) -> PaymentCreatedEvent:
    return PaymentCreatedEvent(
        event_id=uuid4(),
        occurred_at=datetime.now(tz=UTC),
        payment_id=payment.id,
        idempotency_key=payment.idempotency_key,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=dict(payment.metadata_),
        webhook_url=payment.webhook_url,
    )


async def _reload(database: Database, payment_id: UUID) -> Payment:
    async with database.session() as session:
        payment = await session.get(Payment, payment_id)
        assert payment is not None
        return payment


async def test_approved_charge_settles_the_payment_and_calls_the_webhook(
    integration_database: Database,
) -> None:
    payment = await _seed(integration_database, "processor-0001")
    gateway = StubGateway(approved=True)
    dispatcher, captured = _dispatcher(lambda _: httpx.Response(200))

    await _processor(integration_database, gateway, dispatcher).process(_event(payment))

    settled = await _reload(integration_database, payment.id)
    assert settled.status is PaymentStatus.SUCCEEDED
    assert settled.gateway_reference == "ch_stub"
    assert settled.processed_at is not None
    assert settled.processing_attempts == 1

    assert len(captured) == 1
    body = PaymentWebhookPayload.model_validate_json(captured[0].content)
    assert body.status == PaymentStatus.SUCCEEDED.value
    assert body.payment_id == payment.id
    assert body.gateway_reference == "ch_stub"


async def test_declined_charge_marks_the_payment_failed(
    integration_database: Database,
) -> None:
    payment = await _seed(integration_database, "processor-0002")
    gateway = StubGateway(approved=False)
    dispatcher, captured = _dispatcher(lambda _: httpx.Response(200))

    await _processor(integration_database, gateway, dispatcher).process(_event(payment))

    settled = await _reload(integration_database, payment.id)
    assert settled.status is PaymentStatus.FAILED
    assert settled.gateway_reference is None
    assert settled.failure_reason == "card was declined"
    assert len(captured) == 1


async def test_replayed_event_does_not_charge_or_notify_twice(
    integration_database: Database,
) -> None:
    payment = await _seed(integration_database, "processor-0003")
    gateway = StubGateway(approved=True)
    dispatcher, captured = _dispatcher(lambda _: httpx.Response(200))
    processor = _processor(integration_database, gateway, dispatcher)
    event = _event(payment)

    await processor.process(event)
    await processor.process(event)

    assert gateway.calls == [payment.id]
    assert len(captured) == 1
    settled = await _reload(integration_database, payment.id)
    assert settled.processing_attempts == 2


async def test_failed_webhook_delivery_is_recorded_on_the_payment(
    integration_database: Database,
) -> None:
    payment = await _seed(integration_database, "processor-0004")
    gateway = StubGateway(approved=True)
    dispatcher, captured = _dispatcher(lambda _: httpx.Response(503))

    await _processor(integration_database, gateway, dispatcher).process(_event(payment))

    settled = await _reload(integration_database, payment.id)
    assert settled.status is PaymentStatus.SUCCEEDED
    assert settled.webhook_attempts == 3
    assert settled.webhook_last_error == "HTTP 503"
    assert len(captured) == 3


async def test_gateway_failure_leaves_the_payment_pending_for_a_retry(
    integration_database: Database,
) -> None:
    payment = await _seed(integration_database, "processor-0005")
    gateway = StubGateway(raises=TimeoutError("gateway timed out"))
    dispatcher, captured = _dispatcher(lambda _: httpx.Response(200))

    with pytest.raises(TimeoutError):
        await _processor(integration_database, gateway, dispatcher).process(_event(payment))

    pending = await _reload(integration_database, payment.id)
    assert pending.status is PaymentStatus.PENDING
    assert pending.processed_at is None
    assert pending.processing_attempts == 1
    assert captured == []
