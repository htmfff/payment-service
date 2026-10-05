from __future__ import annotations

import pytest
from faststream import AckPolicy, FastStream
from faststream.rabbit import RabbitBroker

from app.config import Settings
from app.messaging.bus import EventPublisher, build_broker
from app.messaging.events import PaymentCreatedEvent
from app.messaging.topology import build_topology
from app.worker.main import _route_failure, build_worker
from tests.conftest import TEST_API_KEY, TEST_WEBHOOK_SECRET


class RecordingPublisher(EventPublisher):
    """Captures what the worker asked the broker to do, without touching RabbitMQ."""

    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.retries: list[int] = []
        self.dead_letters: list[int] = []

    async def publish_payment_created(self, event: PaymentCreatedEvent) -> None:
        raise NotImplementedError

    async def publish_retry(self, event: PaymentCreatedEvent, *, attempt: int, reason: str) -> None:
        if self.fail:
            raise ConnectionError("broker is unreachable")
        self.retries.append(attempt)

    async def publish_dead_letter(
        self, event: PaymentCreatedEvent, *, attempt: int, reason: str
    ) -> None:
        if self.fail:
            raise ConnectionError("broker is unreachable")
        self.dead_letters.append(attempt)


def _event() -> PaymentCreatedEvent:
    return PaymentCreatedEvent.model_validate(
        {
            "event_id": "6f9c2f0e-1f1a-4a1e-9d1f-1c2b3a4d5e6f",
            "occurred_at": "2026-01-15T12:00:00Z",
            "payment_id": "0a3d6b1a-2c4d-4f6a-9b8c-7d6e5f4a3b2c",
            "idempotency_key": "wiring-0001",
            "amount": "10.00",
            "currency": "RUB",
            "description": "Wiring check",
            "metadata": {},
            "webhook_url": "http://receiver.test/hook",
        }
    )


def _worker() -> FastStream:
    return build_worker(
        Settings(
            api_key=TEST_API_KEY,
            webhook_signing_secret=TEST_WEBHOOK_SECRET,
            gateway_min_latency_seconds=0,
            gateway_max_latency_seconds=0,
            log_level="WARNING",
        )
    )


def test_build_worker_wires_a_manually_acknowledged_subscriber() -> None:
    """Guards the `RabbitBroker(ack_policy=MANUAL)` + manual `message.ack()` contract."""
    broker = _worker().broker
    assert isinstance(broker, RabbitBroker)

    subscribers = broker.subscribers
    assert len(subscribers) == 1
    assert subscribers[0].ack_policy is AckPolicy.MANUAL


def test_worker_accepts_the_pinned_faststream_subscriber_signature() -> None:
    """`subscriber(..., no_reply=True)` must stay valid for the pinned FastStream version."""
    settings = Settings(
        api_key=TEST_API_KEY,
        webhook_signing_secret=TEST_WEBHOOK_SECRET,
        log_level="WARNING",
    )
    topology = build_topology(settings.processing_retry_delays)

    broker = build_broker(str(settings.rabbitmq_url), app_id="wiring-check")
    subscriber = broker.subscriber(topology.payments_queue, no_reply=True)

    assert subscriber is not None
    assert subscriber.ack_policy is AckPolicy.MANUAL


@pytest.mark.parametrize(
    ("attempt", "expected_retries", "expected_dead_letters"),
    [(1, [2], []), (2, [3], []), (3, [], [3])],
)
async def test_a_failed_message_walks_the_tiers_then_the_dead_letter_queue(
    attempt: int,
    expected_retries: list[int],
    expected_dead_letters: list[int],
) -> None:
    publisher = RecordingPublisher()

    routed = await _route_failure(
        publisher,
        build_topology((5, 15)),
        _event(),
        attempt,
        RuntimeError("gateway exploded"),
    )

    assert routed is True
    assert publisher.retries == expected_retries
    assert publisher.dead_letters == expected_dead_letters


async def test_an_unroutable_failure_is_reported_back_to_the_caller() -> None:
    """When the broker refuses the republish the worker must dead-letter, not requeue."""
    publisher = RecordingPublisher(fail=True)

    routed = await _route_failure(
        publisher,
        build_topology((5, 15)),
        _event(),
        1,
        RuntimeError("gateway exploded"),
    )

    assert routed is False
    assert publisher.retries == []
