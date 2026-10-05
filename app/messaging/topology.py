from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from aio_pika import ExchangeType, connect_robust
from aio_pika.abc import AbstractChannel
from faststream.rabbit import RabbitExchange, RabbitQueue

from app.core import constants
from app.logging import get_logger

logger = get_logger(__name__)

_QUEUE_TYPE_CLASSIC = "classic"


@dataclass(frozen=True, slots=True)
class Topology:
    """Declarative description of every broker entity this service relies on."""

    retry_delays: tuple[int, ...]
    payments_exchange: RabbitExchange
    payments_queue: RabbitQueue
    dead_letter_exchange: RabbitExchange
    dead_letter_queue: RabbitQueue
    retry_exchange: RabbitExchange
    retry_queues: tuple[RabbitQueue, ...]

    @property
    def retry_tier_count(self) -> int:
        return len(self.retry_queues)


def retry_routing_key(tier: int) -> str:
    return constants.RETRY_ROUTING_KEY_TEMPLATE.format(tier=tier)


def build_topology(retry_delays: Sequence[int]) -> Topology:
    """Retry queues hold a message for their tier delay and then dead-letter it back to the work queue."""
    payments_exchange = RabbitExchange(
        constants.PAYMENTS_EXCHANGE,
        type=ExchangeType.DIRECT,
        durable=True,
    )
    dead_letter_exchange = RabbitExchange(
        constants.DEAD_LETTER_EXCHANGE,
        type=ExchangeType.DIRECT,
        durable=True,
    )
    retry_exchange = RabbitExchange(
        constants.RETRY_EXCHANGE,
        type=ExchangeType.DIRECT,
        durable=True,
    )

    payments_queue = RabbitQueue(
        constants.PAYMENTS_QUEUE,
        durable=True,
        declare=False,
        arguments={
            "x-queue-type": _QUEUE_TYPE_CLASSIC,
            "x-dead-letter-exchange": constants.DEAD_LETTER_EXCHANGE,
            "x-dead-letter-routing-key": constants.PAYMENT_CREATED_ROUTING_KEY,
        },
    )
    dead_letter_queue = RabbitQueue(
        constants.DEAD_LETTER_QUEUE,
        durable=True,
        declare=False,
        arguments={"x-queue-type": _QUEUE_TYPE_CLASSIC},
    )
    retry_queues = tuple(
        RabbitQueue(
            constants.RETRY_QUEUE_NAME_TEMPLATE.format(tier=tier),
            durable=True,
            declare=False,
            arguments={
                "x-queue-type": _QUEUE_TYPE_CLASSIC,
                "x-message-ttl": delay_seconds * 1000,
                "x-dead-letter-exchange": constants.PAYMENTS_EXCHANGE,
                "x-dead-letter-routing-key": constants.PAYMENT_CREATED_ROUTING_KEY,
            },
        )
        for tier, delay_seconds in enumerate(retry_delays, start=1)
    )

    return Topology(
        retry_delays=tuple(retry_delays),
        payments_exchange=payments_exchange,
        payments_queue=payments_queue,
        dead_letter_exchange=dead_letter_exchange,
        dead_letter_queue=dead_letter_queue,
        retry_exchange=retry_exchange,
        retry_queues=retry_queues,
    )


async def ensure_topology(url: str, topology: Topology) -> None:
    """Declare exchanges, queues and bindings before any producer or consumer connects.

    The whole topology is declared here with plain aio_pika, so the `x-arguments` are owned by a
    single module and the FastStream subscribers can attach with `declare=False`.
    """
    connection = await connect_robust(url)
    try:
        channel = await connection.channel()
        try:
            await _declare_entities(channel, topology)
        finally:
            await channel.close()
    finally:
        await connection.close()

    logger.info(
        "rabbitmq topology ready",
        extra={
            "exchange": constants.PAYMENTS_EXCHANGE,
            "queue": constants.PAYMENTS_QUEUE,
            "dead_letter_queue": constants.DEAD_LETTER_QUEUE,
            "retry_tiers": topology.retry_tier_count,
        },
    )


async def _declare_entities(channel: AbstractChannel, topology: Topology) -> None:
    payments_exchange = await channel.declare_exchange(
        topology.payments_exchange.name,
        ExchangeType.DIRECT,
        durable=True,
    )
    dead_letter_exchange = await channel.declare_exchange(
        topology.dead_letter_exchange.name,
        ExchangeType.DIRECT,
        durable=True,
    )
    retry_exchange = await channel.declare_exchange(
        topology.retry_exchange.name,
        ExchangeType.DIRECT,
        durable=True,
    )

    payments_queue = await channel.declare_queue(
        constants.PAYMENTS_QUEUE,
        durable=True,
        arguments=dict(topology.payments_queue.arguments or {}),
    )
    await payments_queue.bind(payments_exchange, routing_key=constants.PAYMENT_CREATED_ROUTING_KEY)

    dead_letter_queue = await channel.declare_queue(
        constants.DEAD_LETTER_QUEUE,
        durable=True,
        arguments=dict(topology.dead_letter_queue.arguments or {}),
    )
    await dead_letter_queue.bind(
        dead_letter_exchange,
        routing_key=constants.PAYMENT_CREATED_ROUTING_KEY,
    )

    for tier, queue in enumerate(topology.retry_queues, start=1):
        declared = await channel.declare_queue(
            queue.name,
            durable=True,
            arguments=dict(queue.arguments or {}),
        )
        await declared.bind(retry_exchange, routing_key=retry_routing_key(tier))