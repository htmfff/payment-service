from __future__ import annotations

import uuid
from collections.abc import Sequence
from datetime import datetime

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import OutboxStatus, PaymentStatus
from app.core.time import utcnow
from app.db.models import OutboxEvent, Payment

_SKIP_SYNC = {"synchronize_session": False}


class PaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert_ignoring_duplicates(self, payment: Payment) -> Payment | None:
        """Insert the payment, or return None if the idempotency key is already used.

        The unique index on `idempotency_key` turns concurrent retries into a no-op instead of
        a 500, so the caller can safely replay the stored payment.
        """
        statement = (
            pg_insert(Payment)
            .values(
                id=payment.id,
                idempotency_key=payment.idempotency_key,
                request_fingerprint=payment.request_fingerprint,
                amount=payment.amount,
                currency=payment.currency,
                description=payment.description,
                metadata_=payment.metadata_,
                webhook_url=payment.webhook_url,
                status=payment.status,
            )
            .on_conflict_do_nothing(index_elements=["idempotency_key"])
            .returning(Payment)
        )
        result = await self._session.execute(statement)
        return result.scalar_one_or_none()

    async def get(self, payment_id: uuid.UUID) -> Payment | None:
        return await self._session.get(Payment, payment_id)

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        statement = select(Payment).where(Payment.idempotency_key == idempotency_key)
        return await self._session.scalar(statement)

    async def mark_processed(
        self,
        payment_id: uuid.UUID,
        *,
        status: PaymentStatus,
        gateway_reference: str | None,
        failure_reason: str | None,
        processed_at: datetime,
    ) -> bool:
        """Move a pending payment to a terminal state.

        Returns False when the payment was already terminal, which makes the consumer safe to
        replay: the guarded `status = pending` predicate is the idempotency barrier.
        """
        statement = (
            update(Payment)
            .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
            .values(
                status=status,
                gateway_reference=gateway_reference,
                failure_reason=failure_reason,
                processed_at=processed_at,
                updated_at=utcnow(),
            )
            .returning(Payment.id)
            .execution_options(**_SKIP_SYNC)
        )
        result = await self._session.execute(statement)
        return result.scalar_one_or_none() is not None

    async def increment_processing_attempts(self, payment_id: uuid.UUID) -> None:
        statement = (
            update(Payment)
            .where(Payment.id == payment_id)
            .values(processing_attempts=Payment.processing_attempts + 1)
            .execution_options(**_SKIP_SYNC)
        )
        await self._session.execute(statement)

    async def record_webhook_attempt(
        self,
        payment_id: uuid.UUID,
        *,
        attempts: int,
        last_error: str | None,
    ) -> None:
        statement = (
            update(Payment)
            .where(Payment.id == payment_id)
            .values(webhook_attempts=attempts, webhook_last_error=last_error)
            .execution_options(**_SKIP_SYNC)
        )
        await self._session.execute(statement)


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def append(
        self,
        *,
        aggregate_id: uuid.UUID,
        event_type: str,
        routing_key: str,
        payload: dict[str, object],
    ) -> OutboxEvent:
        event = OutboxEvent(
            aggregate_id=aggregate_id,
            event_type=event_type,
            routing_key=routing_key,
            payload=payload,
            status=OutboxStatus.PENDING,
            available_at=utcnow(),
        )
        self._session.add(event)
        await self._session.flush()
        return event

    async def lock_publishable_batch(self, limit: int) -> Sequence[OutboxEvent]:
        """Claim a batch of due events with row locks, skipping rows already handled elsewhere.

        `skip_locked` keeps several relay instances from blocking each other, and the caller is
        responsible for committing once the batch is published.
        """
        statement = (
            select(OutboxEvent)
            .where(
                OutboxEvent.status == OutboxStatus.PENDING,
                OutboxEvent.available_at <= utcnow(),
            )
            .order_by(OutboxEvent.available_at, OutboxEvent.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        result = await self._session.execute(statement)
        return tuple(result.scalars().all())

    async def mark_published(self, event_id: uuid.UUID) -> None:
        statement = (
            update(OutboxEvent)
            .where(OutboxEvent.id == event_id)
            .values(
                status=OutboxStatus.PUBLISHED,
                published_at=utcnow(),
                attempts=OutboxEvent.attempts + 1,
                last_error=None,
            )
            .execution_options(**_SKIP_SYNC)
        )
        await self._session.execute(statement)

    async def postpone(
        self,
        event_id: uuid.UUID,
        *,
        next_attempt_at: datetime,
        error: str,
    ) -> None:
        statement = (
            update(OutboxEvent)
            .where(OutboxEvent.id == event_id)
            .values(
                attempts=OutboxEvent.attempts + 1,
                available_at=next_attempt_at,
                last_error=error,
            )
            .execution_options(**_SKIP_SYNC)
        )
        await self._session.execute(statement)

    async def mark_dead(self, event_id: uuid.UUID, *, error: str) -> None:
        statement = (
            update(OutboxEvent)
            .where(OutboxEvent.id == event_id)
            .values(
                status=OutboxStatus.DEAD,
                attempts=OutboxEvent.attempts + 1,
                last_error=error,
            )
            .execution_options(**_SKIP_SYNC)
        )
        await self._session.execute(statement)
