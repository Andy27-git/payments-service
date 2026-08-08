from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

# sqlite не поддерживает SELECT ... FOR UPDATE SKIP LOCKED (не отбрасывает конструкцию,
# но и не реализует настоящую семантику пропуска залоченных строк), поэтому проверка
# конкурентного relay имеет смысл только на настоящем Postgres
pytestmark = pytest.mark.pg

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _probe(url: str) -> str | None:
    """Возвращает None, если к БД удалось подключиться, иначе описание ошибки."""
    import asyncio

    import asyncpg

    async def connect() -> None:
        conn = await asyncpg.connect(url.replace("postgresql+asyncpg://", "postgresql://"))
        await conn.close()

    try:
        asyncio.run(connect())
    except Exception as exc:  # noqa: BLE001 — важен сам факт недоступности, не тип
        return f"{type(exc).__name__}: {exc}"
    return None


@pytest.fixture(scope="module")
def postgres_url() -> str:
    try:
        from testcontainers.community.postgres import PostgresContainer
    except ImportError:
        pytest.skip("testcontainers не установлен")

    try:
        container = PostgresContainer("postgres:16-alpine", driver="asyncpg")
        container.start()
    except Exception as exc:  # noqa: BLE001 — конкретный тип зависит от docker-клиента; любая ошибка здесь = "нет докера", пропускаем тест, а не роняем прогон
        pytest.skip(f"Docker недоступен: {exc}")

    try:
        # testcontainers отдаёт хост как "localhost"; на Docker Desktop он может
        # разрешаться в ::1, куда опубликованный порт не проброшен, и соединение
        # просто отваливается по таймауту. Явный IPv4 обходит это
        url = container.get_connection_url().replace("@localhost:", "@127.0.0.1:")
        # Docker может быть запущен, но недостижим по сети (проброс портов, firewall).
        # Тогда тест бессмысленно падать — это не дефект кода, а окружение
        if problem := _probe(url):
            pytest.skip(f"Postgres из testcontainers недостижим с хоста: {problem}")
        yield url
    finally:
        container.stop()


@pytest.fixture(scope="module")
def migrated_db(postgres_url: str) -> str:
    # прогоняем реальные alembic-миграции отдельным процессом: get_settings() в этом
    # процессе уже закэширован (lru_cache) с sqlite-настройками из conftest.py
    env = {**os.environ, "DATABASE_URL": postgres_url}
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        check=True,
        cwd=PROJECT_ROOT,
        env=env,
    )
    return postgres_url


async def test_concurrent_relay_does_not_publish_row_twice(migrated_db: str):
    from app.messaging import outbox_relay
    from app.models import Outbox

    engine = create_async_engine(migrated_db)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    row_count = 20
    async with session_factory() as session:
        for i in range(row_count):
            session.add(Outbox(event_type="payment.created", payload={"payment_id": f"p-{i}"}))
        await session.commit()

    published_ids: list[str] = []

    async def fake_publish(payload, **_kwargs):
        published_ids.append(payload["payment_id"])

    publish_mock = AsyncMock(side_effect=fake_publish)

    try:
        import unittest.mock

        with unittest.mock.patch.object(outbox_relay.broker, "publish", publish_mock):
            # два relay «гоняются» за одни и те же строки; SKIP LOCKED должен
            # разделить их без пересечений и без потерь
            results = await asyncio.gather(
                outbox_relay.relay_once(session_factory),
                outbox_relay.relay_once(session_factory),
            )
    finally:
        await engine.dispose()

    assert sum(results) == row_count
    assert len(published_ids) == row_count
    assert len(set(published_ids)) == row_count
