from __future__ import annotations

from decimal import Decimal

import pytest
from sqlalchemy import select

from app.models import Outbox
from app.schemas.payment import PaymentCreate
from app.services.exceptions import IdempotencyConflictError
from app.services.payment_service import create_payment


def _payment_data(amount: str = "10.00") -> PaymentCreate:
    return PaymentCreate(
        amount=Decimal(amount),
        currency="RUB",
        description="test",
        webhook_url="http://example.com/hook",
    )


async def test_create_payment_repeat_same_key_same_body_returns_same_payment(session_factory):
    data = _payment_data()

    async with session_factory() as session:
        first = await create_payment(session, "key-1", data)
    async with session_factory() as session:
        second = await create_payment(session, "key-1", data)

    assert first.id == second.id


async def test_create_payment_repeat_same_key_different_body_conflicts(session_factory):
    async with session_factory() as session:
        await create_payment(session, "key-2", _payment_data("10.00"))

    async with session_factory() as session:
        with pytest.raises(IdempotencyConflictError):
            await create_payment(session, "key-2", _payment_data("20.00"))


async def test_create_payment_different_keys_create_different_payments(session_factory):
    async with session_factory() as session:
        first = await create_payment(session, "key-3", _payment_data())
    async with session_factory() as session:
        second = await create_payment(session, "key-4", _payment_data())

    assert first.id != second.id


async def test_create_payment_writes_outbox_row_with_real_payment_id(session_factory):
    # регрессия: payment.id заполняется client-side default'ом только при flush,
    # без явного flush() до сборки payload там оказалась бы строка "None"
    async with session_factory() as session:
        payment = await create_payment(session, "key-outbox", _payment_data())

    async with session_factory() as session:
        outbox_row = (await session.execute(select(Outbox))).scalar_one()

    assert outbox_row.payload == {"payment_id": str(payment.id)}
