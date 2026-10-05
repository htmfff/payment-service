from __future__ import annotations

from typing import Protocol, runtime_checkable

from faststream import AckPolicy
from faststream.rabbit import RabbitBroker, RabbitPublisher

from app.core import constants
from app.messaging.events import PaymentCreatedEvent
from app.messaging.topology import Topology, retry_routing_key

_MAX_HEADER_LENGTH = 512
_JSON_CONTENT_TYPE = "application/json"


@runtime_checkable
class EventPublisher(Protocol):
    """Everything the application needs from the message broker."""

    async def publish_payment_created(self, event: PaymentCreatedEvent) -> None: ...

    async def publish_retry(self, event: PaymentCreatedEvent, *, attempt: int, reason: str) -> None: ...

    async def publish_dead_letter(self, event: PaymentCreatedEvent, *, attempt: int, reason: str) -> None: ...


def build_broker(url: str, *, app_id: str) -> RabbitBroker:
    """Acknowledgement is manual so the consumer decides when a message is really handled."""
    return RabbitBroker(
        url,
        app_id=app_id,
        ack_policy=AckPolicy.MANUAL,
        fail_fast=False,
        reconnect_interval=5,
    )


def _header_value(value: str) -> str:
    return value[:_MAX_HEADER_LENGTH]


class RabbitEventPublisher:
    def __init__(self, broker: RabbitBroker, topology: Topology) -> None:
        self._work = broker.publisher(
            exchange=topology.payments_exchange,
            routing_key=constants.PAYMENT_CREATED_ROUTING_KEY,
            persistent=True,
            content_type=_JSON_CONTENT_TYPE,
        )
        self._retry: tuple[RabbitPublisher, ...] = tuple(
            broker.publisher(
                exchange=topology.retry_exchange,
                routing_key=retry_routing_key(tier),
                persistent=True,
                content_type=_JSON_CONTENT_TYPE,
            )
            for tier in range(1, topology.retry_tier_count + 1)
        )
        self._dead_letter = broker.publisher(
            exchange=topology.dead_letter_exchange,
            routing_key=constants.PAYMENT_CREATED_ROUTING_KEY,
            persistent=True,
            content_type=_JSON_CONTENT_TYPE,
        )

    async def publish_payment_created(self, event: PaymentCreatedEvent) -> None:
        await self._work.publish(
            event.to_message(),
            correlation_id=str(event.payment_id),
            headers={constants.ATTEMPT_HEADER: constants.DEFAULT_ATTEMPT},
        )

    async def publish_retry(self, event: PaymentCreatedEvent, *, attempt: int, reason: str) -> None:
        index = attempt - 1
        if not 0 <= index < len(self._retry):
            raise ValueError(f"retry attempt {attempt} exceeds the configured tier count")
        await self._retry[index].publish(
            event.to_message(),
            correlation_id=str(event.payment_id),
            headers={
                constants.ATTEMPT_HEADER: attempt,
                constants.FAILURE_REASON_HEADER: _header_value(reason),
            },
        )

    async def publish_dead_letter(self, event: PaymentCreatedEvent, *, attempt: int, reason: str) -> None:
        await self._dead_letter.publish(
            event.to_message(),
            correlation_id=str(event.payment_id),
            headers={
                constants.ATTEMPT_HEADER: attempt,
                constants.FAILURE_REASON_HEADER: _header_value(reason),
            },
        )