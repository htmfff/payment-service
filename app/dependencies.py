from __future__ import annotations

import secrets
from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, Header, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.core import constants
from app.core.exceptions import InvalidApiKeyError
from app.db.session import Database


def get_database(request: Request) -> Database:
    return request.app.state.database


def get_app_settings(request: Request) -> Settings:
    return request.app.state.settings


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    async with get_database(request).session() as session:
        yield session


async def require_api_key(
    settings: Annotated[Settings, Depends(get_app_settings)],
    api_key: Annotated[str | None, Header(alias=constants.API_KEY_HEADER)] = None,
) -> None:
    if api_key is None:
        raise InvalidApiKeyError(f"{constants.API_KEY_HEADER} header is required")
    expected = settings.api_key.get_secret_value().encode("utf-8")
    if not secrets.compare_digest(api_key.encode("utf-8"), expected):
        raise InvalidApiKeyError("Provided API key is not valid")


DatabaseDependency = Annotated[Database, Depends(get_database)]
SettingsDependency = Annotated[Settings, Depends(get_app_settings)]
SessionDependency = Annotated[AsyncSession, Depends(get_session)]