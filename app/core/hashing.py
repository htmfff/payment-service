from __future__ import annotations

import hashlib
import hmac
import json
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID


def _normalize_decimal(value: Decimal) -> str:
    if not value.is_finite():
        raise ValueError("Cannot fingerprint a non-finite decimal")
    return format(value.normalize(), "f")


def _normalize(value: Any) -> Any:
    if isinstance(value, Decimal):
        return _normalize_decimal(value)
    if isinstance(value, Enum):
        return _normalize(value.value)
    if isinstance(value, dict):
        return {str(key): _normalize(item) for key, item in sorted(value.items(), key=lambda kv: str(kv[0]))}
    if isinstance(value, (list, tuple)):
        return [_normalize(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    return value


def canonical_json(value: Any) -> str:
    """Serialize a value so that logically equal payloads always produce the same string."""
    return json.dumps(
        _normalize(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )


def fingerprint(value: Any) -> str:
    """Stable digest of a request payload, used to detect Idempotency-Key reuse with a different body."""
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def sign_payload(payload: bytes, secret: str) -> str:
    """Return a hex HMAC-SHA256 signature of the raw request body."""
    return hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()