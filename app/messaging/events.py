from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from app.core.constants import PAYMENT_CREATED_EVENT
from app.core.enums import Currency


class PaymentCreatedEvent(BaseModel):
    """Contract of the `payment.created` message routed to `payments` / `payment.created`."""

    model_config = ConfigDict(frozen=True)

    schema_version: Literal[1] = 1
    event_type: Literal["payment.created"] = PAYMENT_CREATED_EVENT
    event_id: UUID
    occurred_at: datetime
    payment_id: UUID
    idempotency_key: str
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, object] = Field(default_factory=dict)
    webhook_url: str

    def to_message(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_message(cls, raw: bytes) -> PaymentCreatedEvent:
        return cls.model_validate_json(raw)
