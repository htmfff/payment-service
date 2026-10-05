from __future__ import annotations

from random import Random

_JITTER_SOURCE = Random()  # noqa: S311 - spreading retries, not a security decision


def exponential_backoff(
    attempt: int,
    *,
    base_seconds: float,
    multiplier: float = 2.0,
    cap_seconds: float = 300.0,
    jitter_ratio: float = 0.1,
    rng: Random | None = None,
) -> float:
    """Delay before attempt number `attempt` + 1, with jitter so retries do not stampede."""
    if attempt < 1:
        raise ValueError("attempt must be greater than or equal to 1")
    if base_seconds < 0:
        raise ValueError("base_seconds must not be negative")

    delay = min(cap_seconds, base_seconds * multiplier ** (attempt - 1))
    if jitter_ratio <= 0:
        return delay

    source = rng if rng is not None else _JITTER_SOURCE
    spread = delay * jitter_ratio
    return max(0.0, delay + source.uniform(-spread, spread))


def retry_tier_delays(
    max_attempts: int,
    *,
    base_seconds: int,
    multiplier: int = 3,
) -> tuple[int, ...]:
    """Delay tiers for `max_attempts` total attempts: one tier per retry, growing exponentially."""
    if max_attempts < 1:
        raise ValueError("max_attempts must be greater than or equal to 1")
    if base_seconds < 0:
        raise ValueError("base_seconds must not be negative")

    return tuple(base_seconds * multiplier**index for index in range(max_attempts - 1))


def next_retry_tier(attempt: int, tier_count: int) -> int | None:
    """Delay tier to wait in after `attempt` failed, or None once the attempts are exhausted.

    With two tiers and three allowed attempts this maps 1 -> 1, 2 -> 2, 3 -> None.
    """
    if attempt < 1:
        raise ValueError("attempt must be greater than or equal to 1")

    if attempt > tier_count:
        return None
    return attempt
