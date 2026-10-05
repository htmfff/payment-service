from __future__ import annotations

from uuid import UUID


class DomainError(Exception):
    """Base class for every business error raised by the service."""

    code: str = "domain_error"

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class MissingIdempotencyKeyError(DomainError):
    code = "missing_idempotency_key"


class InvalidIdempotencyKeyError(DomainError):
    code = "invalid_idempotency_key"


class IdempotencyKeyConflictError(DomainError):
    code = "idempotency_key_conflict"


class InvalidApiKeyError(DomainError):
    code = "invalid_api_key"


class PaymentNotFoundError(DomainError):
    code = "payment_not_found"

    def __init__(self, payment_id: UUID) -> None:
        super().__init__(f"Payment {payment_id} does not exist")
        self.payment_id = payment_id


class PaymentAlreadyProcessedError(DomainError):
    """Raised when a consumer tries to process a payment that already reached a terminal state."""

    code = "payment_already_processed"


class PaymentDeclinedError(DomainError):
    """The gateway refused the charge. Terminal for the payment, not retriable."""

    code = "payment_declined"


class GatewayUnavailableError(DomainError):
    """The gateway could not be reached. Retriable."""

    code = "gateway_unavailable"


class WebhookDeliveryError(DomainError):
    """All webhook delivery attempts were exhausted."""

    code = "webhook_delivery_failed"

    def __init__(self, message: str, attempts: int) -> None:
        super().__init__(message)
        self.attempts = attempts
