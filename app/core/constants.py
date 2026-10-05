from __future__ import annotations

from typing import Final

APPLICATION_TITLE: Final = "Payment service"
APPLICATION_DESCRIPTION: Final = (
    "Accepts payments, processes them asynchronously through an external gateway "
    "and delivers the outcome to a customer webhook."
)
API_PREFIX: Final = "/api/v1"

API_KEY_HEADER: Final = "X-API-Key"
IDEMPOTENCY_KEY_HEADER: Final = "Idempotency-Key"
IDEMPOTENCY_REPLAY_HEADER: Final = "Idempotency-Replayed"

IDEMPOTENCY_KEY_MIN_LENGTH: Final = 8
IDEMPOTENCY_KEY_MAX_LENGTH: Final = 128

PAYMENT_CREATED_EVENT: Final = "payment.created"

PAYMENTS_EXCHANGE: Final = "payments"
PAYMENT_CREATED_ROUTING_KEY: Final = "payment.created"
PAYMENTS_QUEUE: Final = "payments.new"

DEAD_LETTER_EXCHANGE: Final = "payments.dlx"
DEAD_LETTER_QUEUE: Final = "payments.dlq"

RETRY_EXCHANGE: Final = "payments.retry"
RETRY_ROUTING_KEY_TEMPLATE: Final = "payment.created.retry.{tier}"
RETRY_QUEUE_NAME_TEMPLATE: Final = "payments.retry.{tier}"

ATTEMPT_HEADER: Final = "x-attempt"
FAILURE_REASON_HEADER: Final = "x-failure-reason"
DEFAULT_ATTEMPT: Final = 1

WEBHOOK_SIGNATURE_HEADER: Final = "X-Signature"
WEBHOOK_EVENT_HEADER: Final = "X-Payment-Event"
WEBHOOK_DELIVERY_HEADER: Final = "X-Payment-Delivery"
WEBHOOK_SIGNATURE_ALGORITHM: Final = "sha256"

RETRIABLE_HTTP_STATUSES: Final = frozenset({408, 425, 429, 500, 502, 503, 504})