from __future__ import annotations

import random
from decimal import Decimal
from uuid import uuid4

import pytest

from app.core.enums import Currency
from app.core.exceptions import GatewayUnavailableError, PaymentDeclinedError
from app.services.gateway import EmulatedPaymentGateway


async def _no_sleep(_: float) -> None:
    return None


def _gateway(**overrides: float) -> EmulatedPaymentGateway:
    parameters: dict[str, float] = {
        "min_latency_seconds": 0,
        "max_latency_seconds": 0,
        "success_rate": 1.0,
        "unavailable_rate": 0.0,
    }
    parameters.update(overrides)
    return EmulatedPaymentGateway(
        sleep=_no_sleep,
        rng=random.Random(1),
        **parameters,
    )


async def _charge(gateway: EmulatedPaymentGateway):
    return await gateway.charge(
        payment_id=uuid4(),
        amount=Decimal("100.00"),
        currency=Currency.RUB,
        idempotency_key="idem-0001",
    )


async def test_approved_charge_returns_a_reference() -> None:
    result = await _charge(_gateway())
    assert result.reference.startswith("ch_")
    assert result.processed_at.tzinfo is not None


async def test_zero_success_rate_always_declines() -> None:
    with pytest.raises(PaymentDeclinedError):
        await _charge(_gateway(success_rate=0.0))


async def test_full_unavailability_raises_a_retriable_error() -> None:
    with pytest.raises(GatewayUnavailableError):
        await _charge(_gateway(unavailable_rate=1.0))


async def test_latency_is_drawn_from_the_configured_window() -> None:
    observed: list[float] = []

    async def record(delay: float) -> None:
        observed.append(delay)

    gateway = EmulatedPaymentGateway(
        min_latency_seconds=2,
        max_latency_seconds=5,
        success_rate=1.0,
        sleep=record,
        rng=random.Random(3),
    )
    for _ in range(20):
        await _charge(gateway)

    assert len(observed) == 20
    assert all(2 <= delay <= 5 for delay in observed)
