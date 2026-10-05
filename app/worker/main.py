from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import httpx
from faststream import FastStream
from faststream.rabbit import RabbitMessage

from app.config import Settings, get_settings
from app.core import constants
from app.core.retries import next_retry_tier
from app.db.session import Database
from app.logging import configure_logging, get_logger
from app.messaging.bus import EventPublisher, RabbitEventPublisher, build_broker
from app.messaging.events import PaymentCreatedEvent
from app.messaging.topology import Topology, build_topology, ensure_topology
from app.services.gateway import EmulatedPaymentGateway
from app.services.webhooks import WebhookDispatcher, build_webhook_client
from app.worker.handlers import PaymentProcessor, read_attempt

logger = get_logger(__name__)


def build_processor(settings: Settings, database: Database) -> tuple[PaymentProcessor, httpx.AsyncClient]:
    """Build the processor and the HTTP client it needs, returned together so the caller can close it."""
    http_client = build_webhook_client()
    webhooks = WebhookDispatcher(
        http_client,
        signing_secret=settings.webhook_signing_secret.get_secret_value(),
        max_attempts=settings.webhook_max_attempts,
        timeout_seconds=settings.webhook_timeout_seconds,
        backoff_base_seconds=settings.webhook_backoff_base_seconds,
    )
    gateway = EmulatedPaymentGateway(
        min_latency_seconds=settings.gateway_min_latency_seconds,
        max_latency_seconds=settings.gateway_max_latency_seconds,
        success_rate=settings.gateway_success_rate,
        unavailable_rate=settings.gateway_unavailable_rate,
    )
    return PaymentProcessor(database, gateway, webhooks), http_client


def build_worker(settings: Settings) -> FastStream:
    topology = build_topology(settings.processing_retry_delays)
    broker = build_broker(
        str(settings.rabbitmq_url),
        app_id=f"{settings.application_name}-consumer",
    )
    publisher = RabbitEventPublisher(broker, topology)
    database = Database(
        settings.database_url,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
    )
    processor, http_client = build_processor(settings, database)

    @asynccontextmanager
    async def lifespan(_: Any) -> AsyncIterator[None]:
        await ensure_topology(str(settings.rabbitmq_url), topology)
        try:
            yield
        finally:
            await http_client.aclose()
            await database.dispose()

    app = FastStream(broker, logger=logger, lifespan=lifespan)

    @broker.subscriber(topology.payments_queue, topology.payments_exchange, no_reply=True)
    async def handle_payment_created(
        event: PaymentCreatedEvent,
        message: RabbitMessage,
    ) -> None:
        attempt = read_attempt(message.headers)
        logger.info(
            "payment.created received",
            extra={"payment_id": str(event.payment_id), "attempt": attempt},
        )
        try:
            await processor.process(event)
        except Exception as exc:
            if not await _route_failure(publisher, topology, event, attempt, exc):
                await message.nack(requeue=True)
                return
        await message.ack()

    return app


async def _route_failure(
    publisher: EventPublisher,
    topology: Topology,
    event: PaymentCreatedEvent,
    attempt: int,
    error: Exception,
) -> bool:
    """Republish a failed message into the next delay tier, or into the DLQ once attempts run out.

    Returns False when the message could not be routed, so the caller can leave the original
    unacknowledged and let RabbitMQ redeliver it.
    """
    reason = f"{type(error).__name__}: {error}"
    tier = next_retry_tier(attempt, topology.retry_tier_count)
    context = {
        "payment_id": str(event.payment_id),
        "attempt": attempt,
        "reason": reason,
    }

    try:
        if tier is None:
            await publisher.publish_dead_letter(event, attempt=attempt, reason=reason)
            logger.error(
                "payment message moved to the dead letter queue",
                extra={**context, "queue": constants.DEAD_LETTER_QUEUE},
            )
        else:
            await publisher.publish_retry(event, attempt=tier + 1, reason=reason)
            logger.warning(
                "payment message scheduled for a retry",
                extra={
                    **context,
                    "next_attempt": tier + 1,
                    "delay_seconds": topology.retry_delays[tier - 1],
                },
            )
    except Exception:
        logger.exception("failed to route a failed payment message", extra=context)
        return False

    return True


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    app = build_worker(settings)
    asyncio.run(app.run(log_level=getattr(logging, settings.log_level, logging.INFO)))


if __name__ == "__main__":
    main()