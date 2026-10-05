from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.core.exceptions import (
    DomainError,
    IdempotencyKeyConflictError,
    InvalidApiKeyError,
    InvalidIdempotencyKeyError,
    MissingIdempotencyKeyError,
    PaymentNotFoundError,
)
from app.logging_config import get_logger

logger = get_logger(__name__)

HTTP_STATUS_BY_ERROR: dict[type[DomainError], int] = {
    PaymentNotFoundError: status.HTTP_404_NOT_FOUND,
    IdempotencyKeyConflictError: status.HTTP_409_CONFLICT,
    InvalidApiKeyError: status.HTTP_401_UNAUTHORIZED,
    MissingIdempotencyKeyError: status.HTTP_400_BAD_REQUEST,
    InvalidIdempotencyKeyError: status.HTTP_400_BAD_REQUEST,
}
FALLBACK_STATUS = status.HTTP_400_BAD_REQUEST


def error_body(code: str, message: str, details: dict[str, Any] | None = None) -> dict[str, Any]:
    return {"error": {"code": code, "message": message, "details": details}}


async def handle_domain_error(request: Request, exc: Exception) -> JSONResponse:
    if not isinstance(exc, DomainError):
        raise exc

    http_status = HTTP_STATUS_BY_ERROR.get(type(exc), FALLBACK_STATUS)
    headers = {"WWW-Authenticate": "ApiKey"} if isinstance(exc, InvalidApiKeyError) else None

    if http_status >= status.HTTP_500_INTERNAL_SERVER_ERROR:
        logger.exception("unhandled domain error", extra={"path": request.url.path})
    else:
        logger.info(
            "request rejected",
            extra={"path": request.url.path, "code": exc.code, "status": http_status},
        )

    return JSONResponse(
        status_code=http_status,
        content=error_body(exc.code, exc.message),
        headers=headers,
    )


async def handle_validation_error(request: Request, exc: Exception) -> JSONResponse:
    if not isinstance(exc, RequestValidationError):
        raise exc

    details = [
        {
            "loc": [str(part) for part in error.get("loc", ())],
            "msg": str(error.get("msg", "")),
            "type": str(error.get("type", "")),
        }
        for error in exc.errors()
    ]
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=error_body("validation_error", "Request payload is invalid", {"errors": details}),
    )


async def handle_http_exception(request: Request, exc: Exception) -> JSONResponse:
    if not isinstance(exc, StarletteHTTPException):
        raise exc

    return JSONResponse(
        status_code=exc.status_code,
        content=error_body("http_error", str(exc.detail)),
        headers=getattr(exc, "headers", None),
    )


async def handle_unexpected_error(request: Request, exc: Exception) -> JSONResponse:
    logger.exception("unhandled exception", extra={"path": request.url.path})
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=error_body("internal_error", "Unexpected server error"),
    )


def install_exception_handlers(app: FastAPI) -> None:
    app.add_exception_handler(DomainError, handle_domain_error)
    app.add_exception_handler(RequestValidationError, handle_validation_error)
    app.add_exception_handler(StarletteHTTPException, handle_http_exception)
    app.add_exception_handler(Exception, handle_unexpected_error)
