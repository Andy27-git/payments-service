from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.health import router as health_router
from app.api.v1.payments import router as payments_router
from app.config import get_settings
from app.logging import setup_logging


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    setup_logging(get_settings().log_level)
    yield


app = FastAPI(title="Payments Service", lifespan=lifespan)
app.include_router(health_router)
app.include_router(payments_router)
