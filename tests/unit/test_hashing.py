from __future__ import annotations

from decimal import Decimal

from app.core.hashing import canonical_json, fingerprint, sign_payload


def test_fingerprint_ignores_key_order() -> None:
    first = fingerprint({"amount": Decimal("10.00"), "currency": "RUB", "tags": ["a", "b"]})
    second = fingerprint({"tags": ["a", "b"], "currency": "RUB", "amount": Decimal("10.00")})
    assert first == second


def test_fingerprint_normalises_decimal_scale() -> None:
    assert fingerprint({"amount": Decimal("1490.5")}) == fingerprint({"amount": Decimal("1490.50")})


def test_fingerprint_detects_a_different_amount() -> None:
    assert fingerprint({"amount": Decimal("1490.50")}) != fingerprint({"amount": Decimal("1490.51")})


def test_fingerprint_keeps_list_order_significant() -> None:
    assert fingerprint({"tags": ["a", "b"]}) != fingerprint({"tags": ["b", "a"]})


def test_canonical_json_is_compact_and_sorted() -> None:
    assert canonical_json({"b": 1, "a": "x"}) == '{"a":"x","b":1}'


def test_sign_payload_is_stable_and_secret_dependent() -> None:
    body = b'{"event":"payment.succeeded"}'
    first = sign_payload(body, "secret")
    assert first == sign_payload(body, "secret")
    assert first != sign_payload(body, "other-secret")