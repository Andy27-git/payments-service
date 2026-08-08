from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

from sqlalchemy import select

from app.messaging import outbox_relay
from app.models import Outbox


async def test_relay_once_publishes_unpublished_rows_and_marks_them(session_factory, monkeypatch):
    publish_mock = AsyncMock()
    monkeypatch.setattr(outbox_relay.broker, "publish", publish_mock)

    async with session_factory() as session:
        session.add(Outbox(event_type="payment.created", payload={"payment_id": "p-1"}))
        session.add(Outbox(event_type="payment.created", payload={"payment_id": "p-2"}))
        await session.commit()

    published = await outbox_relay.relay_once(session_factory)

    assert published == 2
    assert publish_mock.await_count == 2

    async with session_factory() as session:
        rows = (await session.execute(select(Outbox))).scalars().all()
        assert all(row.published_at is not None for row in rows)


async def test_relay_once_skips_already_published_rows(session_factory, monkeypatch):
    publish_mock = AsyncMock()
    monkeypatch.setattr(outbox_relay.broker, "publish", publish_mock)

    async with session_factory() as session:
        session.add(Outbox(event_type="payment.created", payload={"payment_id": "p-1"}))
        await session.commit()

    first_run = await outbox_relay.relay_once(session_factory)
    second_run = await outbox_relay.relay_once(session_factory)

    assert first_run == 1
    assert second_run == 0
    assert publish_mock.await_count == 1


async def test_relay_once_returns_zero_when_nothing_to_publish(session_factory, monkeypatch):
    publish_mock = AsyncMock()
    monkeypatch.setattr(outbox_relay.broker, "publish", publish_mock)

    published = await outbox_relay.relay_once(session_factory)

    assert published == 0
    publish_mock.assert_not_awaited()


async def test_cleanup_published_removes_only_old_published_rows(session_factory):
    now = datetime.now(UTC)
    async with session_factory() as session:
        session.add(
            Outbox(
                event_type="payment.created",
                payload={"payment_id": "old"},
                published_at=now - timedelta(hours=48),
            )
        )
        session.add(
            Outbox(
                event_type="payment.created",
                payload={"payment_id": "recent"},
                published_at=now - timedelta(hours=1),
            )
        )
        # неопубликованную запись уборка не должна трогать ни при каких условиях
        session.add(Outbox(event_type="payment.created", payload={"payment_id": "unpublished"}))
        await session.commit()

    deleted = await outbox_relay.cleanup_published(session_factory, retention_hours=24)

    assert deleted == 1
    async with session_factory() as session:
        remaining = (await session.execute(select(Outbox))).scalars().all()
        assert {row.payload["payment_id"] for row in remaining} == {"recent", "unpublished"}


async def test_cleanup_published_disabled_by_zero_retention(session_factory):
    async with session_factory() as session:
        session.add(
            Outbox(
                event_type="payment.created",
                payload={"payment_id": "old"},
                published_at=datetime.now(UTC) - timedelta(days=365),
            )
        )
        await session.commit()

    deleted = await outbox_relay.cleanup_published(session_factory, retention_hours=0)

    assert deleted == 0
    async with session_factory() as session:
        assert len((await session.execute(select(Outbox))).scalars().all()) == 1
