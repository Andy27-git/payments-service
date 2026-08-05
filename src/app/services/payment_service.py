from __future__ import annotations

import hashlib
import json

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Outbox, Payment
from app.repositories.payment_repository import PaymentRepository
from app.schemas.payment import PaymentCreate
from app.services.exceptions import IdempotencyConflictError


def _request_hash(data: PaymentCreate) -> str:
    # mode="json" приводит Decimal/HttpUrl к строкам, sort_keys даёт стабильный хэш
    # независимо от порядка полей в теле запроса
    canonical = json.dumps(data.model_dump(mode="json"), sort_keys=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


async def create_payment(session: AsyncSession, idempotency_key: str, data: PaymentCreate) -> Payment:
    repo = PaymentRepository(session)
    request_hash = _request_hash(data)

    payment = Payment(
        amount=data.amount,
        currency=data.currency,
        description=data.description,
        metadata_=data.metadata,
        webhook_url=str(data.webhook_url),
        idempotency_key=idempotency_key,
        request_hash=request_hash,
    )
    repo.add(payment)
    # outbox-запись пишется в той же транзакции, что и сам платёж (outbox pattern):
    # либо оба insert'а закоммитятся вместе, либо ни один — событие никогда не потеряется
    # и никогда не появится для несуществующего платежа
    session.add(Outbox(event_type="payment.created", payload={"payment_id": str(payment.id)}))

    try:
        await session.commit()
    except IntegrityError:
        # уникальный индекс на idempotency_key сработал — значит запрос с этим ключом уже был
        await session.rollback()
        existing = await repo.get_by_idempotency_key(idempotency_key)
        if existing is None:
            # конфликт словили, но строку не нашли — не наш случай, пробрасываем дальше
            raise
        if existing.request_hash != request_hash:
            raise IdempotencyConflictError(idempotency_key) from None
        return existing

    # created_at заполняется в БД через server_default, а не в Python-объекте;
    # expire_on_commit=False (см. db/session.py) не подтягивает его сам — нужен явный refresh
    await session.refresh(payment)
    return payment
