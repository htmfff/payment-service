from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field, field_validator

from app.core.enums import Currency, PaymentStatus
from app.db.models import Payment

MAX_DESCRIPTION_LENGTH = 255
MAX_AMOUNT = Decimal("9999999999999999.99")
MAX_DECIMAL_PLACES = 2


class PaymentCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Decimal = Field(
        gt=0,
        le=MAX_AMOUNT,
        description="Positive amount, at most 2 decimal places",
        examples=["1490.50"],
    )
    currency: Currency = Field(description="ISO currency code", examples=["RUB"])
    description: str = Field(
        min_length=1,
        max_length=MAX_DESCRIPTION_LENGTH,
        description="Human readable payment purpose",
        examples=["Order #A-10293"],
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Free-form data echoed back in the webhook",
        examples=[{"order_id": "A-10293"}],
    )
    webhook_url: AnyHttpUrl = Field(
        description="Endpoint notified once the payment reaches a terminal state",
        examples=["http://localhost:8080/hooks/payments"],
    )

    @field_validator("amount")
    @classmethod
    def _validate_amount(cls, value: Decimal) -> Decimal:
        if not value.is_finite():
            raise ValueError("amount must be a finite number")
        if -value.as_tuple().exponent > MAX_DECIMAL_PLACES:
            raise ValueError(f"amount must not have more than {MAX_DECIMAL_PLACES} decimal places")
        return value

    @field_validator("description")
    @classmethod
    def _validate_description(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("description must not be blank")
        return stripped


class PaymentAcceptedResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    payment_id: UUID
    status: PaymentStatus
    created_at: datetime


class PaymentResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    payment_id: UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    webhook_url: str
    status: PaymentStatus
    gateway_reference: str | None
    failure_reason: str | None
    processing_attempts: int
    created_at: datetime
    updated_at: datetime
    processed_at: datetime | None


class ErrorBody(BaseModel):
    code: str
    message: str
    details: dict[str, Any] | None = None


class ErrorResponse(BaseModel):
    error: ErrorBody


class HealthResponse(BaseModel):
    status: str
    service: str
    checks: dict[str, str] = Field(default_factory=dict)


def payment_accepted(payment: Payment) -> PaymentAcceptedResponse:
    return PaymentAcceptedResponse(
        payment_id=payment.id,
        status=payment.status,
        created_at=payment.created_at,
    )


def payment_details(payment: Payment) -> PaymentResponse:
    return PaymentResponse(
        payment_id=payment.id,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=dict(payment.metadata_),
        webhook_url=payment.webhook_url,
        status=payment.status,
        gateway_reference=payment.gateway_reference,
        failure_reason=payment.failure_reason,
        processing_attempts=payment.processing_attempts,
        created_at=payment.created_at,
        updated_at=payment.updated_at,
        processed_at=payment.processed_at,
    )