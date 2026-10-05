from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta

from app.config import Settings
from app.core.retries import exponential_backoff
from app.core.time import utcnow
from app.db.models import OutboxEvent
from app.db.repositories import OutboxRepository
from app.db.session import Database, transaction
from app.logging_config import get_logger
from app.messaging.bus import EventPublisher
from app.messaging.events import PaymentCreatedEvent

logger = get_logger(__name__)

SleepCallable = Callable[[float], Awaitable[None]]
_MAX_FAILURE_LENGTH = 500
_MAX_BACKOFF_SECONDS = 300.0


class OutboxRelay:
    """Publishes outbox rows to the broker, at least once.

    The relay claims a batch with `FOR UPDATE SKIP LOCKED`, publishes it and flips the rows to
    `published` in the same transaction. A crash between publish and commit only causes a
    duplicate delivery, which the consumer tolerates, rather than a lost event.
    """

    def __init__(
        self,
        database: Database,
        publisher: EventPublisher,
        settings: Settings,
        *,
        sleep: SleepCallable = asyncio.sleep,
    ) -> None:
        self._database = database
        self._publisher = publisher
        self._settings = settings
        self._sleep = sleep

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                published = await self.run_once()
            except Exception:
                logger.exception("outbox relay iteration failed, retrying after the poll interval")
                published = 0
            if published == 0:
                await self._sleep(self._settings.outbox_poll_interval_seconds)

    async def run_once(self) -> int:
        async with self._database.session() as session, transaction(session):
            repository = OutboxRepository(session)
            events = await repository.lock_publishable_batch(self._settings.outbox_batch_size)
            published = 0
            for event in events:
                if await self._dispatch(event, repository):
                    published += 1
            return published

    async def _dispatch(self, event: OutboxEvent, repository: OutboxRepository) -> bool:
        try:
            payload = PaymentCreatedEvent.model_validate(event.payload)
            await self._publisher.publish_payment_created(payload)
        except Exception as exc:
            await self._record_failure(event, repository, exc)
            return False

        await repository.mark_published(event.id)
        return True

    async def _record_failure(
        self,
        event: OutboxEvent,
        repository: OutboxRepository,
        error: Exception,
    ) -> None:
        message = f"{type(error).__name__}: {error}"[:_MAX_FAILURE_LENGTH]
        attempts = event.attempts + 1

        if attempts >= self._settings.outbox_max_attempts:
            await repository.mark_dead(event.id, error=message)
            logger.error(
                "outbox event exhausted its attempts and will not be republished",
                extra={"event_id": str(event.id), "attempts": attempts, "error": message},
            )
            return

        delay = exponential_backoff(
            attempts,
            base_seconds=self._settings.outbox_retry_base_seconds,
            multiplier=2.0,
            cap_seconds=_MAX_BACKOFF_SECONDS,
        )
        await repository.postpone(
            event.id,
            next_attempt_at=utcnow() + timedelta(seconds=delay),
            error=message,
        )
        logger.warning(
            "outbox publish deferred",
            extra={
                "event_id": str(event.id),
                "attempts": attempts,
                "retry_in_seconds": round(delay, 3),
                "error": message,
            },
        )
