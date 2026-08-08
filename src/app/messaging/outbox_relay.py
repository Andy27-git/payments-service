from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import get_settings
from app.messaging.broker import NEW_ROUTING_KEY, broker, payments_exchange
from app.models import Outbox

logger = logging.getLogger(__name__)

POLL_INTERVAL_SECONDS = 0.5
BATCH_SIZE = 20
CLEANUP_INTERVAL_SECONDS = 600


async def relay_once(session_factory: async_sessionmaker[AsyncSession]) -> int:
    """Публикует одну пачку неотправленных outbox-записей, возвращает их число."""
    async with session_factory() as session:
        stmt = (
            select(Outbox)
            .where(Outbox.published_at.is_(None))
            .order_by(Outbox.created_at)
            .limit(BATCH_SIZE)
            # SKIP LOCKED: при нескольких запущенных consumer'ах каждый разбирает свою
            # порцию строк вместо того, чтобы блокироваться на строках, занятых другим
            .with_for_update(skip_locked=True)
        )
        rows = (await session.execute(stmt)).scalars().all()
        if not rows:
            return 0

        for row in rows:
            # публикуем ДО commit: если процесс упадёт между publish и commit, при
            # перезапуске строка всё ещё published_at=NULL и будет опубликована повторно —
            # дублирующая доставка безопасна благодаря идемпотентности consumer'а.
            # Обратный порядок (commit, потом publish) в случае падения потерял бы событие
            # безвозвратно — этого outbox pattern как раз обязан избегать.
            await broker.publish(
                row.payload,
                exchange=payments_exchange,
                routing_key=NEW_ROUTING_KEY,
                persist=True,
            )
            row.published_at = datetime.now(UTC)

        await session.commit()
        return len(rows)


async def cleanup_published(
    session_factory: async_sessionmaker[AsyncSession], retention_hours: int
) -> int:
    """Удаляет опубликованные outbox-записи старше retention; возвращает число удалённых."""
    if retention_hours <= 0:
        return 0
    cutoff = datetime.now(UTC) - timedelta(hours=retention_hours)
    async with session_factory() as session:
        result = await session.execute(
            delete(Outbox).where(Outbox.published_at.is_not(None), Outbox.published_at < cutoff)
        )
        await session.commit()
        return result.rowcount


async def run_outbox_relay(session_factory: async_sessionmaker[AsyncSession]) -> None:
    """Бесконечный поллер outbox-таблицы; предполагается запуск как отдельная asyncio-задача."""
    retention_hours = get_settings().outbox_retention_hours
    next_cleanup = asyncio.get_running_loop().time()
    while True:
        try:
            published = await relay_once(session_factory)
        except Exception:
            logger.exception("outbox relay: batch failed, will retry next tick")
            published = 0

        # уборка идёт в том же цикле, но по своему таймеру: отдельная задача ради
        # одного DELETE раз в 10 минут не нужна, а публикацию она не задерживает —
        # запросы к БД здесь короткие
        now = asyncio.get_running_loop().time()
        if now >= next_cleanup:
            next_cleanup = now + CLEANUP_INTERVAL_SECONDS
            try:
                deleted = await cleanup_published(session_factory, retention_hours)
            except Exception:
                logger.exception("outbox relay: cleanup failed, will retry next cycle")
            else:
                if deleted:
                    logger.info("outbox relay: cleaned up published rows", extra={"deleted": deleted})

        if not published:
            await asyncio.sleep(POLL_INTERVAL_SECONDS)
