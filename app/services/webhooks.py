from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict

from app.core import constants
from app.core.hashing import canonical_json, sign_payload
from app.core.retries import exponential_backoff
from app.core.time import utcnow
from app.db.models import Payment
from app.logging import get_logger

logger = get_logger(__name__)

SleepCallable = Callable[[float], Awaitable[None]]
_MAX_FAILURE_LENGTH = 500
_WEBHOOK_BACKOFF_CAP_SECONDS = 30.0


class PaymentWebhookPayload(BaseModel):
    """Body delivered to the customer endpoint once a payment reaches a terminal state."""

    model_config = ConfigDict(frozen=True)

    event: str
    payment_id: uuid.UUID
    status: str
    amount: Decimal
    currency: str
    description: str
    metadata: dict[str, Any]
    gateway_reference: str | None
    failure_reason: str | None
    created_at: datetime
    processed_at: datetime

    def encode(self) -> bytes:
        return canonical_json(self.model_dump(mode="json")).encode("utf-8")


def webhook_payload_from_payment(payment: Payment) -> PaymentWebhookPayload:
    processed_at = payment.processed_at or utcnow()
    return PaymentWebhookPayload(
        event=f"payment.{payment.status.value}",
        payment_id=payment.id,
        status=payment.status.value,
        amount=payment.amount,
        currency=payment.currency.value,
        description=payment.description,
        metadata=dict(payment.metadata_),
        gateway_reference=payment.gateway_reference,
        failure_reason=payment.failure_reason,
        created_at=payment.created_at,
        processed_at=processed_at,
    )


@dataclass(frozen=True, slots=True)
class WebhookDeliveryResult:
    delivered: bool
    attempts: int
    last_error: str | None


class WebhookDispatcher:
    """Posts the outcome to the customer endpoint, retrying with exponential backoff.

    Delivery failures never fail the message: the payment is already terminal in the database,
    so the outcome is recorded and surfaced through logs and the `webhook_*` columns instead.
    """

    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        signing_secret: str,
        max_attempts: int,
        timeout_seconds: float,
        backoff_base_seconds: float,
        sleep: SleepCallable = asyncio.sleep,
    ) -> None:
        self._client = client
        self._signing_secret = signing_secret
        self._max_attempts = max_attempts
        self._timeout_seconds = timeout_seconds
        self._backoff_base_seconds = backoff_base_seconds
        self._sleep = sleep

    async def deliver(self, url: str, payload: PaymentWebhookPayload) -> WebhookDeliveryResult:
        body = payload.encode()
        signature = sign_payload(body, self._signing_secret)
        headers = {
            "Content-Type": "application/json",
            constants.WEBHOOK_SIGNATURE_HEADER: (
                f"{constants.WEBHOOK_SIGNATURE_ALGORITHM}={signature}"
            ),
            constants.WEBHOOK_EVENT_HEADER: payload.event,
            constants.WEBHOOK_DELIVERY_HEADER: str(payload.payment_id),
        }

        last_error: str | None = None
        attempts = 0

        for attempt in range(1, self._max_attempts + 1):
            if attempt > 1:
                await self._sleep(
                    exponential_backoff(
                        attempt - 1,
                        base_seconds=self._backoff_base_seconds,
                        multiplier=2.0,
                        cap_seconds=_WEBHOOK_BACKOFF_CAP_SECONDS,
                    ),
                )

            attempts = attempt
            try:
                response = await self._client.post(
                    url,
                    content=body,
                    headers=headers,
                    timeout=self._timeout_seconds,
                )
            except httpx.HTTPError as exc:
                last_error = f"{type(exc).__name__}: {exc}"[:_MAX_FAILURE_LENGTH]
                logger.warning(
                    "webhook attempt failed",
                    extra={"attempt": attempt, "url": url, "error": last_error},
                )
                continue

            if 200 <= response.status_code < 300:
                logger.info(
                    "webhook delivered",
                    extra={"attempt": attempt, "url": url, "status_code": response.status_code},
                )
                return WebhookDeliveryResult(delivered=True, attempts=attempts, last_error=None)

            last_error = f"HTTP {response.status_code}"[:_MAX_FAILURE_LENGTH]
            logger.warning(
                "webhook rejected",
                extra={"attempt": attempt, "url": url, "status_code": response.status_code},
            )

            if response.status_code not in constants.RETRIABLE_HTTP_STATUSES:
                logger.error(
                    "webhook endpoint refused the delivery permanently",
                    extra={"url": url, "status_code": response.status_code},
                )
                break

        return WebhookDeliveryResult(delivered=False, attempts=attempts, last_error=last_error)


def build_webhook_client(max_connections: int = 50) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        follow_redirects=False,
        limits=httpx.Limits(max_connections=max_connections, max_keepalive_connections=10),
    )