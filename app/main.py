from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI, HTTPException, status

from app import __version__
from app.api.errors import install_exception_handlers
from app.api.router import api_router
from app.api.schemas import HealthResponse
from app.config import Settings, get_settings
from app.core import constants
from app.db.session import Database
from app.logging import configure_logging, get_logger
from app.messaging.bus import RabbitEventPublisher, build_broker
from app.messaging.topology import build_topology, ensure_topology
from app.services.outbox import OutboxRelay

logger = get_logger(__name__)

_RELAY_SHUTDOWN_TIMEOUT_SECONDS = 5.0


def create_app(settings: Settings | None = None) -> FastAPI:
    resolved = settings or get_settings()
    configure_logging(resolved.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        database = Database(
            resolved.database_url,
            pool_size=resolved.db_pool_size,
            max_overflow=resolved.db_max_overflow,
        )
        app.state.settings = resolved
        app.state.database = database

        topology = build_topology(resolved.processing_retry_delays)
        with suppress(Exception):
            await ensure_topology(str(resolved.rabbitmq_url), topology)

        broker = build_broker(
            str(resolved.rabbitmq_url),
            app_id=f"{resolved.application_name}-api",
        )
        await broker.connect()
        app.state.publisher = RabbitEventPublisher(broker, topology)

        stop_event = asyncio.Event()
        relay = OutboxRelay(database, app.state.publisher, resolved)
        relay_task = asyncio.create_task(relay.run(stop_event), name="outbox-relay")

        logger.info(
            "payment api ready",
            extra={
                "environment": resolved.environment.value,
                "retry_delays_seconds": list(resolved.processing_retry_delays),
            },
        )

        try:
            yield
        finally:
            stop_event.set()
            with suppress(asyncio.TimeoutError, asyncio.CancelledError):
                await asyncio.wait_for(relay_task, timeout=_RELAY_SHUTDOWN_TIMEOUT_SECONDS)
            relay_task.cancel()
            await broker.close()
            await database.dispose()
            logger.info("payment api stopped")

    app = FastAPI(
        title=constants.APPLICATION_TITLE,
        description=constants.APPLICATION_DESCRIPTION,
        version=__version__,
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
    )

    install_exception_handlers(app)
    app.include_router(api_router, prefix=constants.API_PREFIX)

    @app.get("/health/live", response_model=HealthResponse, tags=["health"])
    async def liveness() -> HealthResponse:
        return HealthResponse(status="ok", service=resolved.application_name)

    @app.get("/health/ready", response_model=HealthResponse, tags=["health"])
    async def readiness() -> HealthResponse:
        if not await app.state.database.ping():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="database is unreachable",
            )
        return HealthResponse(
            status="ok",
            service=resolved.application_name,
            checks={"database": "ok"},
        )

    return app


api = create_app()

__all__ = ("api", "create_app")