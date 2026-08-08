from __future__ import annotations

import asyncio
import contextlib
import logging

from faststream import FastStream

from app.config import get_settings
from app.consumer.handlers import (
    handle_payment_new,  # noqa: F401  регистрирует subscriber payments.new
)
from app.db.session import get_session_factory
from app.logging import setup_logging
from app.messaging.broker import broker, declare_topology
from app.messaging.outbox_relay import run_outbox_relay

logger = logging.getLogger(__name__)

# logger=... по той же причине, что и у broker (см. app/messaging/broker.py):
# дефолтный логгер FastStream пишет текстом мимо JSON-хендлера
app = FastStream(broker, logger=logging.getLogger("app.consumer"))

_relay_task: asyncio.Task[None] | None = None


@app.on_startup
async def _on_startup() -> None:
    setup_logging(get_settings().log_level)
    # соединение поднимаем здесь явно: declare_topology нужен канал, а broker.start()
    # (объявляющий очередь подписчика payments.new) выполняется уже после on_startup
    await broker.connect()
    await declare_topology()


@app.after_startup
async def _start_relay() -> None:
    # к этому моменту broker.start() уже отработал и consumer готов разбирать publish'и,
    # поэтому фоновый relay безопасно запускать только сейчас
    global _relay_task
    _relay_task = asyncio.create_task(run_outbox_relay(get_session_factory()))


@app.on_shutdown
async def _stop_relay() -> None:
    if _relay_task is None:
        return
    _relay_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await _relay_task


if __name__ == "__main__":
    asyncio.run(app.run())
