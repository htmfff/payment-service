from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.api.schemas import PaymentCreateRequest
from app.core.enums import Currency
from app.worker.handlers import read_attempt

_VALID_PAYLOAD: dict[str, object] = {
    "amount": "1490.50",
    "currency": "RUB",
    "description": "Order #A-10293",
    "metadata": {"order_id": "A-10293"},
    "webhook_url": "http://localhost:8080/hooks/payments",
}


def test_amount_is_parsed_into_a_decimal() -> None:
    request = PaymentCreateRequest.model_validate(_VALID_PAYLOAD)
    assert str(request.amount) == "1490.50"
    assert request.currency is Currency.RUB


def test_metadata_defaults_to_an_empty_object() -> None:
    payload = {key: value for key, value in _VALID_PAYLOAD.items() if key != "metadata"}
    assert PaymentCreateRequest.model_validate(payload).metadata == {}


@pytest.mark.parametrize(
    "amount",
    ["0", "-10.00", "10.555", "abc"],
)
def test_invalid_amounts_are_rejected(amount: str) -> None:
    with pytest.raises(ValidationError):
        PaymentCreateRequest.model_validate({**_VALID_PAYLOAD, "amount": amount})


def test_unknown_fields_are_rejected() -> None:
    with pytest.raises(ValidationError):
        PaymentCreateRequest.model_validate({**_VALID_PAYLOAD, "extra": "nope"})


def test_blank_description_is_rejected() -> None:
    with pytest.raises(ValidationError):
        PaymentCreateRequest.model_validate({**_VALID_PAYLOAD, "description": "   "})


def test_non_http_webhook_is_rejected() -> None:
    with pytest.raises(ValidationError):
        PaymentCreateRequest.model_validate({**_VALID_PAYLOAD, "webhook_url": "file:///etc/passwd"})


@pytest.mark.parametrize(
    ("headers", "expected"),
    [
        (None, 1),
        ({}, 1),
        ({"x-attempt": 3}, 3),
        ({"x-attempt": "2"}, 2),
        ({"x-attempt": "nonsense"}, 1),
        ({"x-attempt": 0}, 1),
        ({"x-attempt": None}, 1),
        ({"x-attempt": True}, 1),
    ],
)
def test_read_attempt_falls_back_to_the_first_delivery(
    headers: dict[str, object] | None,
    expected: int,
) -> None:
    assert read_attempt(headers) == expected
