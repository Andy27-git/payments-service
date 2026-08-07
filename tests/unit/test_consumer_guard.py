from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from app.consumer import handlers
from app.models import Payment, PaymentStatus


async def _make_payment(session_factory, **overrides) -> uuid.UUID:
    async with session_factory() as session:
        payment = Payment(
            amount=Decimal("10.00"),
            currency="RUB",
            description="test",
            webhook_url="http://example.com/hook",
            idempotency_key=str(uuid.uuid4()),
            request_hash="hash",
            **overrides,
        )
        session.add(payment)
        await session.commit()
        return payment.id


async def test_process_skips_fully_delivered_payment(session_factory, monkeypatch):
    monkeypatch.setattr(handlers, "get_session_factory", lambda: session_factory)
    gateway_mock = AsyncMock()
    webhook_mock = AsyncMock()
    monkeypatch.setattr(handlers, "process_payment", gateway_mock)
    monkeypatch.setattr(handlers, "send_webhook", webhook_mock)

    payment_id = await _make_payment(
        session_factory,
        status=PaymentStatus.SUCCEEDED,
        webhook_delivered_at=datetime.now(UTC),
    )

    await handlers._process(payment_id)

    gateway_mock.assert_not_awaited()
    webhook_mock.assert_not_awaited()


async def test_process_retries_webhook_for_terminal_undelivered_payment(session_factory, monkeypatch):
    # гард шага 2: сообщение, вернувшееся из retry-очереди именно из-за неудачного
    # вебхука, видит уже терминальный status — но обязано повторить отправку вебхука,
    # а не выйти молча (иначе вебхук не переотправился бы никогда)
    monkeypatch.setattr(handlers, "get_session_factory", lambda: session_factory)
    gateway_mock = AsyncMock()
    webhook_mock = AsyncMock(return_value=True)
    monkeypatch.setattr(handlers, "process_payment", gateway_mock)
    monkeypatch.setattr(handlers, "send_webhook", webhook_mock)

    payment_id = await _make_payment(
        session_factory,
        status=PaymentStatus.SUCCEEDED,
        webhook_delivered_at=None,
    )

    await handlers._process(payment_id)

    gateway_mock.assert_not_awaited()
    webhook_mock.assert_awaited_once()

    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
        assert payment.webhook_delivered_at is not None
        assert payment.webhook_attempts == 1


async def test_process_pending_payment_runs_gateway_and_webhook(session_factory, monkeypatch):
    monkeypatch.setattr(handlers, "get_session_factory", lambda: session_factory)
    gateway_mock = AsyncMock(return_value=PaymentStatus.SUCCEEDED)
    webhook_mock = AsyncMock(return_value=True)
    monkeypatch.setattr(handlers, "process_payment", gateway_mock)
    monkeypatch.setattr(handlers, "send_webhook", webhook_mock)

    payment_id = await _make_payment(session_factory, status=PaymentStatus.PENDING)

    await handlers._process(payment_id)

    gateway_mock.assert_awaited_once()
    webhook_mock.assert_awaited_once()

    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
        assert payment.status == PaymentStatus.SUCCEEDED
        assert payment.processed_at is not None
        assert payment.webhook_delivered_at is not None


async def test_process_raises_when_webhook_delivery_fails(session_factory, monkeypatch):
    monkeypatch.setattr(handlers, "get_session_factory", lambda: session_factory)
    monkeypatch.setattr(handlers, "process_payment", AsyncMock(return_value=PaymentStatus.SUCCEEDED))
    monkeypatch.setattr(handlers, "send_webhook", AsyncMock(return_value=False))

    payment_id = await _make_payment(session_factory, status=PaymentStatus.PENDING)

    with pytest.raises(handlers.WebhookDeliveryError):
        await handlers._process(payment_id)
