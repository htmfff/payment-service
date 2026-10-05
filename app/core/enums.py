from __future__ import annotations

from enum import StrEnum


class Currency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class OutboxStatus(StrEnum):
    PENDING = "pending"
    PUBLISHED = "published"
    DEAD = "dead"


TERMINAL_PAYMENT_STATUSES: frozenset[PaymentStatus] = frozenset(
    {PaymentStatus.SUCCEEDED, PaymentStatus.FAILED}
)


class Environment(StrEnum):
    LOCAL = "local"
    DOCKER = "docker"
    STAGING = "staging"
    PRODUCTION = "production"