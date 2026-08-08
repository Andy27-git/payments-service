from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime
from typing import Any

from faststream.rabbit import Channel, RabbitMessage
from sqlalchemy import update

from app.db.session import get_session_factory
from app.gateway.emulator import process_payment
from app.messaging.broker import (
    DLQ_ROUTING_KEY,
    MAX_ATTEMPTS,
    RETRY_QUEUE_NAMES,
    broker,
    dlx_exchange,
    new_queue,
)
from app.models import Payment, PaymentStatus
from app.webhook.sender import send_webhook

logger = logging.getLogger(__name__)

RETRY_COUNT_HEADER = "x-retry-count"

# обработка одного сообщения занимает 2-5с (эмуляция шлюза); при дефолтном prefetch=1
# consumer обрабатывал бы сообщения строго по одному, отсюда пропускная способность ~0.25 msg/s
PREFETCH_COUNT = 10


class WebhookDeliveryError(Exception):
    """Технический сбой доставки вебхука — сигнал для ретрая, не бизнес-провал платежа."""


@broker.subscriber(new_queue, channel=Channel(prefetch_count=PREFETCH_COUNT))
async def handle_payment_new(body: dict[str, Any], message: RabbitMessage) -> None:
    attempt = int(message.headers.get(RETRY_COUNT_HEADER, 0))

    try:
        payment_id = uuid.UUID(body["payment_id"])
        await _process(payment_id)
    except Exception:
        # любая техническая ошибка (БД недоступна, вебхук вернул 5xx/таймаут, неразбираемый
        # payload) уходит по единому retry-пути; исходное сообщение при этом всё равно
        # ack'аем ниже — повтор организуем сами через отдельные очереди, а не через nack брокера
        logger.exception("payment payload %s: processing failed on attempt %s", body, attempt)
        await _schedule_retry(body, attempt)


async def _process(payment_id: uuid.UUID) -> None:
    # Каждый шаг — отдельная короткая сессия, и ни одна не живёт во время внешних
    # вызовов. Держать одну сессию на весь хендлер нельзя: первый же SELECT открывает
    # транзакцию, и она оставалась бы открытой все 2-5с эмуляции шлюза плюс время
    # HTTP-запроса вебхука. При prefetch_count=10 это до 10 соединений в состоянии
    # idle in transaction — они держат снапшот и мешают вакууму на ровном месте.
    session_factory = get_session_factory()

    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
        if payment is None:
            return
        needs_gateway = payment.status == PaymentStatus.PENDING
        if not needs_gateway and payment.webhook_delivered_at is not None:
            # платёж уже полностью обработан и вебхук доставлен — это дубликат/поздний
            # повтор доставки. Проверять только status нельзя: сообщение, вернувшееся из
            # retry-очереди именно из-за неудачного вебхука, увидело бы уже проставленный
            # status и вышло бы здесь, так и не переотправив вебхук
            return

    if needs_gateway:
        outcome = await process_payment()
        async with session_factory() as session:
            result = await session.execute(
                update(Payment)
                .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
                .values(status=outcome, processed_at=datetime.now(UTC))
            )
            await session.commit()
            updated = result.rowcount
        if updated == 0:
            # параллельная доставка того же сообщения уже перевела платёж из pending
            return

    async with session_factory() as session:
        # инкремент до HTTP-вызова: попытка должна учитываться, даже если процесс
        # упадёт в момент отправки вебхука
        await session.execute(
            update(Payment)
            .where(Payment.id == payment_id)
            .values(webhook_attempts=Payment.webhook_attempts + 1)
        )
        await session.commit()
        # перечитываем платёж уже с финальным статусом — именно он уходит в payload
        # вебхука (объект остаётся пригодным после закрытия сессии: expire_on_commit=False,
        # все нужные атрибуты загружены этим SELECT'ом)
        payment = await session.get(Payment, payment_id)
        if payment is None:
            # платёж исчез между двумя чтениями — в этом сервисе строки не удаляются,
            # так что случай гипотетический; проверка нужна, чтобы не улететь в
            # AttributeError на None вместо явного выхода
            return

    delivered = await send_webhook(payment)
    if not delivered:
        raise WebhookDeliveryError(f"webhook delivery failed for payment {payment_id}")

    async with session_factory() as session:
        await session.execute(
            update(Payment)
            .where(Payment.id == payment_id)
            .values(webhook_delivered_at=datetime.now(UTC))
        )
        await session.commit()


async def _schedule_retry(body: dict[str, Any], attempt: int) -> None:
    if attempt >= MAX_ATTEMPTS:
        # x-retry-count пробрасываем и в DLQ: иначе по лежащему там сообщению не видно,
        # сколько попыток за ним стоит
        await broker.publish(
            body,
            exchange=dlx_exchange,
            routing_key=DLQ_ROUTING_KEY,
            headers={RETRY_COUNT_HEADER: attempt},
            persist=True,
        )
        return
    await broker.publish(
        body,
        queue=RETRY_QUEUE_NAMES[attempt],
        headers={RETRY_COUNT_HEADER: attempt + 1},
        persist=True,
    )
