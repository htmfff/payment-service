from __future__ import annotations

import os
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import httpx
import pytest
from alembic import command
from alembic.config import Config as AlembicConfig
from sqlalchemy import text

from app.config import Settings
from app.db.session import Database
from app.main import create_app

PROJECT_ROOT = Path(__file__).resolve().parents[1]

TEST_API_KEY = "test-api-key"
TEST_WEBHOOK_SECRET = "test-webhook-secret"
MIGRATED_URLS: set[str] = set()


@pytest.fixture
def settings() -> Settings:
    return Settings(
        api_key=TEST_API_KEY,
        webhook_signing_secret=TEST_WEBHOOK_SECRET,
        gateway_min_latency_seconds=0,
        gateway_max_latency_seconds=0,
        log_level="WARNING",
    )


@pytest.fixture(scope="session")
def test_database_url() -> Iterator[str]:
    url = os.getenv("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL is not configured, integration tests are skipped")
    if url not in MIGRATED_URLS:
        _upgrade_database(url)
        MIGRATED_URLS.add(url)
    yield url


def _upgrade_database(url: str) -> None:
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = url
    try:
        config = AlembicConfig(str(PROJECT_ROOT / "alembic.ini"))
        config.set_main_option("script_location", str(PROJECT_ROOT / "alembic"))
        command.upgrade(config, "head")
    finally:
        if previous is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = previous


@pytest.fixture
async def integration_database(test_database_url: str) -> AsyncIterator[Database]:
    database = Database(test_database_url)
    try:
        async with database.engine.begin() as connection:
            await connection.execute(text("TRUNCATE outbox_events, payments CASCADE"))
    except Exception as error:
        pytest.skip(f"test database is unreachable: {error}")

    yield database
    await database.dispose()


@pytest.fixture
async def api_client(
    settings: Settings,
    integration_database: Database,
) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(settings)
    app.state.settings = settings
    app.state.database = integration_database

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client


@pytest.fixture
def auth_headers() -> dict[str, str]:
    return {"X-API-Key": TEST_API_KEY}
