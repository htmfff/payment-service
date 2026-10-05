from __future__ import annotations

import asyncio
import random
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Protocol

from app.core.enums import Currency
from app.core.exceptions import GatewayUnavailableError, PaymentDeclinedError
from app.core.time import utcnow

SleepCallable = Callable[[float], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class ChargeResult:
    reference: str
    processed_at: datetime


class PaymentGateway(Protocol):
    async def charge(
        self,
        *,
        payment_id: uuid.UUID,
        amount: Decimal,
        currency: Currency,
        idempotency_key: str,
    ) -> ChargeResult: ...


class EmulatedPaymentGateway:
    """Stand-in for an external payment service provider.

    Latency and approval rate are configurable so the demo behaves like the real thing: 2-5
    seconds of "processing", roughly nine out of ten charges approved. The `unavailable_rate`
    knob produces retriable failures, which is what exercises the retry tiers and the DLQ.
    """

    def __init__(
        self,
        *,
        min_latency_seconds: float,
        max_latency_seconds: float,
        success_rate: float,
        unavailable_rate: float = 0.0,
        sleep: SleepCallable = asyncio.sleep,
        rng: random.Random | None = None,
    ) -> None:
        self._min_latency_seconds = min_latency_seconds
        self._max_latency_seconds = max_latency_seconds
        self._success_rate = success_rate
        self._unavailable_rate = unavailable_rate
        self._sleep = sleep
        self._rng = rng or random.Random()

    async def charge(
        self,
        *,
        payment_id: uuid.UUID,
        amount: Decimal,
        currency: Currency,
        idempotency_key: str,
    ) -> ChargeResult:
        await self._sleep(
            self._rng.uniform(self._min_latency_seconds, self._max_latency_seconds),
        )

        if self._rng.random() < self._unavailable_rate:
            raise GatewayUnavailableError("Gateway responded with a temporary failure")

        if self._rng.random() >= self._success_rate:
            raise PaymentDeclinedError("Gateway declined the charge")

        return ChargeResult(reference=f"ch_{uuid.uuid4().hex[:24]}", processed_at=utcnow())