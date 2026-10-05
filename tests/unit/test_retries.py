from __future__ import annotations

from random import Random

import pytest

from app.core.retries import (
    exponential_backoff,
    next_retry_tier,
    retry_tier_delays,
)


def test_exponential_backoff_doubles_until_the_cap() -> None:
    delays = [
        exponential_backoff(attempt, base_seconds=2.0, multiplier=2.0, jitter_ratio=0)
        for attempt in range(1, 5)
    ]
    assert delays == [2.0, 4.0, 8.0, 16.0]


def test_exponential_backoff_never_exceeds_the_cap() -> None:
    delay = exponential_backoff(10, base_seconds=2.0, multiplier=2.0, cap_seconds=30, jitter_ratio=0)
    assert delay == 30


def test_exponential_backoff_jitter_stays_inside_the_ratio() -> None:
    rng = Random(7)
    for attempt in (1, 2, 3):
        baseline = exponential_backoff(attempt, base_seconds=10.0, jitter_ratio=0)
        sampled = exponential_backoff(
            attempt,
            base_seconds=10.0,
            jitter_ratio=0.2,
            rng=rng,
        )
        assert baseline * 0.8 <= sampled <= baseline * 1.2


def test_retry_tier_delays_grow_exponentially() -> None:
    assert retry_tier_delays(4, base_seconds=5, multiplier=3) == (5, 15, 45)
    assert retry_tier_delays(1, base_seconds=5, multiplier=3) == ()


def test_next_retry_tier_maps_attempts_to_delay_tiers() -> None:
    assert next_retry_tier(1, tier_count=2) == 1
    assert next_retry_tier(2, tier_count=2) == 2
    assert next_retry_tier(3, tier_count=2) is None


@pytest.mark.parametrize("attempt", [0, -1])
def test_next_retry_tier_rejects_non_positive_attempts(attempt: int) -> None:
    with pytest.raises(ValueError, match="attempt"):
        next_retry_tier(attempt, tier_count=2)


def test_exponential_backoff_rejects_non_positive_attempts() -> None:
    with pytest.raises(ValueError, match="attempt"):
        exponential_backoff(0, base_seconds=1.0)