from __future__ import annotations

from datetime import UTC, datetime


def utcnow() -> datetime:
    """Timezone-aware current time, single source of clock reads across the service."""
    return datetime.now(UTC)