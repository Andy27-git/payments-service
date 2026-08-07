from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from app.consumer import handlers
from app.messaging.broker import DLQ_ROUTING_KEY, MAX_ATTEMPTS, RETRY_QUEUE_NAMES


@pytest.mark.parametrize("attempt", range(MAX_ATTEMPTS))
async def test_schedule_retry_publishes_to_matching_retry_queue(monkeypatch, attempt):
    publish_mock = AsyncMock()
    monkeypatch.setattr(handlers.broker, "publish", publish_mock)
    body = {"payment_id": "abc"}

    await handlers._schedule_retry(body, attempt)

    publish_mock.assert_awaited_once_with(
        body,
        queue=RETRY_QUEUE_NAMES[attempt],
        headers={handlers.RETRY_COUNT_HEADER: attempt + 1},
        persist=True,
    )


async def test_schedule_retry_routes_to_dlq_after_max_attempts(monkeypatch):
    publish_mock = AsyncMock()
    monkeypatch.setattr(handlers.broker, "publish", publish_mock)
    body = {"payment_id": "abc"}

    await handlers._schedule_retry(body, MAX_ATTEMPTS)

    publish_mock.assert_awaited_once()
    _, kwargs = publish_mock.await_args
    assert kwargs["routing_key"] == DLQ_ROUTING_KEY
    assert kwargs["exchange"] is handlers.dlx_exchange
