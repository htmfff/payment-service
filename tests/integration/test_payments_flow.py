from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import func, select

from app.config import Settings
from app.core.enums import Currency, OutboxStatus, PaymentStatus
from app.db.models import OutboxEvent, Payment
from app.db.repositories import OutboxRepository
from app.db.session import Database, transaction
from app.messaging.events import PaymentCreatedEvent
from app.services.outbox import OutboxRelay

pytestmark = pytest.mark.integration

_WEBHOOK_URL = "http://localhost:8080/hooks/payments"
_PAYMENT_BODY: dict[str, object] = {
    "amount": "1490.50",
    "currency": "RUB",
    "description": "Order #A-10293",
    "metadata": {"order_id": "A-10293"},
    "webhook_url": _WEBHOOK_URL,
}


class RecordingPublisher:
    """Stands in for RabbitMQ so the relay can be asserted without a broker."""

    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.published: list[PaymentCreatedEvent] = []

    async def publish_payment_created(self, event: PaymentCreatedEvent) -> None:
        if self.fail:
            raise ConnectionError("broker is down")
        self.published.append(event)

    async def publish_retry(self, event: PaymentCreatedEvent, *, attempt: int, reason: str) -> None:
        raise AssertionError("not used in this test")

    async def publish_dead_letter(self, event: PaymentCreatedEvent, *, attempt: int, reason: str) -> None:
        raise AssertionError("not used in this test")


async def _create_payment(
    client: httpx.AsyncClient,
    headers: dict[str, str],
    idempotency_key: str,
    body: dict[str, object] | None = None,
) -> httpx.Response:
    return await client.post(
        "/api/v1/payments",
        json=body or _PAYMENT_BODY,
        headers={**headers, "Idempotency-Key": idempotency_key},
    )


async def _pending_outbox_rows(database: Database) -> int:
    async with database.session() as session:
        result = await session.execute(
            select(func.count())
            .select_from(OutboxEvent)
            .where(OutboxEvent.status == OutboxStatus.PENDING),
        )
        return int(result.scalar_one())


async def _seed_payment(database: Database, idempotency_key: str) -> Payment:
    async with database.session() as session:
        async with transaction(session):
            payment = Payment(
                idempotency_key=idempotency_key,
                request_fingerprint="f" * 64,
                amount=Decimal("10.00"),
                currency=Currency.RUB,
                description="Seeded payment",
                metadata_={},
                webhook_url=_WEBHOOK_URL,
                status=PaymentStatus.PENDING,
            )
            session.add(payment)
            await session.flush()
            await OutboxRepository(session).append(
                aggregate_id=payment.id,
                event_type="payment.created",
                routing_key="payment.created",
                payload={
                    "event_id": str(uuid4()),
                    "occurred_at": "2026-01-15T12:00:00Z",
                    "payment_id": str(payment.id),
                    "idempotency_key": idempotency_key,
                    "amount": "10.00",
                    "currency": "RUB",
                    "description": "Seeded payment",
                    "metadata": {},
                    "webhook_url": _WEBHOOK_URL,
                },
            )
            return payment


async def test_create_payment_is_accepted_and_writes_one_outbox_event(
    api_client: httpx.AsyncClient,
    auth_headers: dict[str, str],
    integration_database: Database,
) -> None:
    response = await _create_payment(api_client, auth_headers, "create-me-0001")

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == PaymentStatus.PENDING.value
    assert response.headers["Idempotency-Replayed"] == "false"
    assert response.headers["Location"].endswith(body["payment_id"])
    assert await _pending_outbox_rows(integration_database) == 1


async def test_repeated_idempotency_key_returns_the_same_payment(
    api_client: httpx.AsyncClient,
    auth_headers: dict[str, str],
    integration_database: Database,
) -> None:
    first = await _create_payment(api_client, auth_headers, "create-me-0002")
    second = await _create_payment(api_client, auth_headers, "create-me-0002")

    assert first.status_code == 202
    assert second.status_code == 202
    assert second.json()["payment_id"] == first.json()["payment_id"]
    assert second.headers["Idempotency-Replayed"] == "true"
    assert await _pending_outbox_rows(integration_database) == 1


async def test_same_key_with_a_different_body_is_rejected(
    api_client: httpx.AsyncClient,
    auth_headers: dict[str, str],
) -> None:
    await _create_payment(api_client, auth_headers, "create-me-0003")

    conflict = await _create_payment(
        api_client,
        auth_headers,
        "create-me-0003",
        {**_PAYMENT_BODY, "amount": "10.00"},
    )

    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "idempotency_key_conflict"


async def test_payment_details_reflect_the_stored_payment(
    api_client: httpx.AsyncClient,
    auth_headers: dict[str, str],
) -> None:
    created = await _create_payment(api_client, auth_headers, "create-me-0004")
    payment_id = created.json()["payment_id"]

    fetched = await api_client.get(f"/api/v1/payments/{payment_id}", headers=auth_headers)

    assert fetched.status_code == 200
    body = fetched.json()
    assert body["payment_id"] == payment_id
    assert body["amount"] == "1490.50"
    assert body["currency"] == "RUB"
    assert body["metadata"] == {"order_id": "A-10293"}
    assert body["status"] == "pending"
    assert body["processed_at"] is None


async def test_unknown_payment_returns_404(
    api_client: httpx.AsyncClient,
    auth_headers: dict[str, str],
) -> None:
    response = await api_client.get(f"/api/v1/payments/{uuid4()}", headers=auth_headers)

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "payment_not_found"


async def test_missing_idempotency_key_returns_400(
    api_client: httpx.AsyncClient,
    auth_headers: dict[str, str],
) -> None:
    response = await api_client.post("/api/v1/payments", json=_PAYMENT_BODY, headers=auth_headers)

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "missing_idempotency_key"


async def test_short_idempotency_key_returns_400(
    api_client: httpx.AsyncClient,
    auth_headers: dict[str, str],
) -> None:
    response = await _create_payment(api_client, auth_headers, "short")

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_idempotency_key"


async def test_api_key_is_required(api_client: httpx.AsyncClient) -> None:
    response = await _create_payment(api_client, {}, "create-me-0005")

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "invalid_api_key"


async def test_invalid_api_key_is_rejected(api_client: httpx.AsyncClient) -> None:
    response = await _create_payment(api_client, {"X-API-Key": "wrong-key"}, "create-me-0006")

    assert response.status_code == 401


async def test_invalid_amount_returns_422(
    api_client: httpx.AsyncClient,
    auth_headers: dict[str, str],
) -> None:
    response = await _create_payment(
        api_client,
        auth_headers,
        "create-me-0007",
        {**_PAYMENT_BODY, "amount": "10.555"},
    )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"


async def test_outbox_relay_publishes_and_marks_the_event(
    integration_database: Database,
) -> None:
    payment = await _seed_payment(integration_database, "relay-0001")
    publisher = RecordingPublisher()

    assert await OutboxRelay(integration_database, publisher, Settings()).run_once() == 1
    assert [event.payment_id for event in publisher.published] == [payment.id]

    async with integration_database.session() as session:
        event = (await session.execute(select(OutboxEvent))).scalars().one()
    assert event.status is OutboxStatus.PUBLISHED
    assert event.published_at is not None
    assert event.attempts == 1


async def test_outbox_relay_defers_when_the_broker_is_unavailable(
    integration_database: Database,
) -> None:
    await _seed_payment(integration_database, "relay-0002")
    publisher = RecordingPublisher(fail=True)

    assert await OutboxRelay(integration_database, publisher, Settings()).run_once() == 0

    async with integration_database.session() as session:
        event = (await session.execute(select(OutboxEvent))).scalars().one()
    assert event.status is OutboxStatus.PENDING
    assert event.attempts == 1
    assert event.last_error is not None
    assert event.available_at > event.created_at


async def test_outbox_relay_marks_the_event_dead_after_the_attempt_budget(
    integration_database: Database,
) -> None:
    await _seed_payment(integration_database, "relay-0003")
    settings = Settings(outbox_max_attempts=1)
    publisher = RecordingPublisher(fail=True)

    assert await OutboxRelay(integration_database, publisher, settings).run_once() == 0

    async with integration_database.session() as session:
        event = (await session.execute(select(OutboxEvent))).scalars().one()
    assert event.status is OutboxStatus.DEAD