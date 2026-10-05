from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, Response, status

from app.api.schemas import (
    PaymentAcceptedResponse,
    PaymentCreateRequest,
    PaymentResponse,
    payment_accepted,
    payment_details,
)
from app.core import constants
from app.core.exceptions import (
    InvalidIdempotencyKeyError,
    MissingIdempotencyKeyError,
    PaymentNotFoundError,
)
from app.dependencies import SessionDependency, require_api_key
from app.services.payments import CreatePaymentCommand, PaymentService

router = APIRouter(
    prefix="/payments",
    tags=["payments"],
    dependencies=[Depends(require_api_key)],
)

_COMMON_RESPONSES: dict[int | str, dict[str, object]] = {
    status.HTTP_400_BAD_REQUEST: {"description": "Missing or malformed Idempotency-Key"},
    status.HTTP_401_UNAUTHORIZED: {"description": "Missing or invalid X-API-Key"},
    status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Request payload is invalid"},
}


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentAcceptedResponse,
    summary="Create a payment",
    responses={
        status.HTTP_409_CONFLICT: {"description": "Idempotency-Key reused with a different body"},
        **_COMMON_RESPONSES,
    },
)
async def create_payment(
    payload: PaymentCreateRequest,
    response: Response,
    session: SessionDependency,
    idempotency_key: Annotated[
        str | None,
        Header(alias=constants.IDEMPOTENCY_KEY_HEADER),
    ] = None,
) -> PaymentAcceptedResponse:
    key = _validated_idempotency_key(idempotency_key)

    outcome = await PaymentService(session).create(
        CreatePaymentCommand(
            amount=payload.amount,
            currency=payload.currency,
            description=payload.description,
            metadata=payload.metadata,
            webhook_url=str(payload.webhook_url),
        ),
        idempotency_key=key,
    )

    payment = outcome.payment
    response.headers["Location"] = f"{constants.API_PREFIX}{router.prefix}/{payment.id}"
    response.headers[constants.IDEMPOTENCY_REPLAY_HEADER] = str(outcome.replayed).lower()
    return payment_accepted(payment)


@router.get(
    "/{payment_id}",
    response_model=PaymentResponse,
    summary="Get payment details",
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Payment does not exist"},
        **_COMMON_RESPONSES,
    },
)
async def get_payment(payment_id: UUID, session: SessionDependency) -> PaymentResponse:
    payment = await PaymentService(session).get(payment_id)
    if payment is None:
        raise PaymentNotFoundError(payment_id)
    return payment_details(payment)


def _validated_idempotency_key(raw_key: str | None) -> str:
    if raw_key is None or not raw_key.strip():
        raise MissingIdempotencyKeyError(
            f"{constants.IDEMPOTENCY_KEY_HEADER} header is required",
        )

    key = raw_key.strip()
    if not constants.IDEMPOTENCY_KEY_MIN_LENGTH <= len(key) <= constants.IDEMPOTENCY_KEY_MAX_LENGTH:
        raise InvalidIdempotencyKeyError(
            "Idempotency-Key must be between "
            f"{constants.IDEMPOTENCY_KEY_MIN_LENGTH} and "
            f"{constants.IDEMPOTENCY_KEY_MAX_LENGTH} characters",
        )
    return key
