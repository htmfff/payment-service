from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import httpx
import pytest

from app.core import constants
from app.core.hashing import sign_payload
from app.services.webhooks import (
    PaymentWebhookPayload,
    WebhookDeliveryResult,
    WebhookDispatcher,
)

_URL = "http://localhost:8080/hooks/payments"


async def _no_sleep(_: float) -> None:
    return None


def _payload() -> PaymentWebhookPayload:
    moment = datetime(2026, 1, 15, 12, 0, tzinfo=UTC)
    return PaymentWebhookPayload(
        event="payment.succeeded",
        payment_id=uuid4(),
        status="succeeded",
        amount=Decimal("1490.50"),
        currency="RUB",
        description="Order #A-10293",
        metadata={"order_id": "A-10293"},
        gateway_reference="ch_abc",
        failure_reason=None,
        created_at=moment,
        processed_at=moment,
    )


def _dispatcher(
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    max_attempts: int = 3,
) -> WebhookDispatcher:
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return WebhookDispatcher(
        client,
        signing_secret="secret",
        max_attempts=max_attempts,
        timeout_seconds=1.0,
        backoff_base_seconds=0,
        sleep=_no_sleep,
    )


async def test_webhook_is_signed_and_delivered() -> None:
    payload = _payload()
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(204)

    result = await _dispatcher(handler).deliver(_URL, payload)

    assert result == WebhookDeliveryResult(delivered=True, attempts=1, last_error=None)
    assert len(captured) == 1
    expected = sign_payload(captured[0].content, "secret")
    assert captured[0].headers[constants.WEBHOOK_SIGNATURE_HEADER] == f"sha256={expected}"
    assert captured[0].headers[constants.WEBHOOK_EVENT_HEADER] == "payment.succeeded"


async def test_retriable_status_is_retried_until_it_succeeds() -> None:
    responses = [httpx.Response(503), httpx.Response(500), httpx.Response(200)]
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return responses[len(calls) - 1]

    result = await _dispatcher(handler).deliver(_URL, _payload())

    assert result.delivered is True
    assert result.attempts == 3
    assert len(calls) == 3


async def test_permanent_status_is_not_retried() -> None:
    calls = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(422)

    result = await _dispatcher(handler).deliver(_URL, _payload())

    assert result.delivered is False
    assert result.attempts == 1
    assert result.last_error == "HTTP 422"


async def test_transport_errors_exhaust_the_attempt_budget() -> None:
    calls = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise httpx.ConnectError("connection refused", request=None)

    result = await _dispatcher(handler, max_attempts=2).deliver(_URL, _payload())

    assert result.delivered is False
    assert result.attempts == 2
    assert calls == 2
    assert result.last_error is not None


async def test_single_attempt_budget() -> None:
    result = await _dispatcher(
        lambda _: httpx.Response(200),
        max_attempts=1,
    ).deliver(_URL, _payload())

    assert result.delivered is True


@pytest.mark.parametrize("status_code", [500, 502, 503, 504, 408, 425, 429])
def test_retryable_statuses_cover_the_usual_transient_failures(status_code: int) -> None:
    assert status_code in constants.RETRIABLE_HTTP_STATUSES
