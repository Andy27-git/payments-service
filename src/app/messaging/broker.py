from __future__ import annotations

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.config import get_settings

NEW_ROUTING_KEY = "payments.new"
DLQ_ROUTING_KEY = "payments.new.dlq"

# ttl в мс для трёх попыток ретрая с экспоненциальной задержкой; после третьей — DLQ
RETRY_DELAYS_MS = (2_000, 4_000, 8_000)
MAX_ATTEMPTS = len(RETRY_DELAYS_MS)

broker = RabbitBroker(get_settings().rabbitmq_url)

payments_exchange = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
dlx_exchange = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)

new_queue = RabbitQueue(NEW_ROUTING_KEY, durable=True, routing_key=NEW_ROUTING_KEY)

# retry-очереди ни к чему не привязаны (никто их не consume'ит напрямую) — сообщение
# в них публикуется через default exchange по имени очереди, "живёт" там ttl миллисекунд,
# а затем RabbitMQ сам перекладывает его назад в payments.new через x-dead-letter-*.
# Отдельная очередь на каждую задержку (а не одна с per-message TTL) нужна, чтобы TTL
# истёк у головы очереди раньше, чем у сообщений позади неё (per-message TTL в общей очереди
# создал бы head-of-line blocking: RabbitMQ проверяет истечение TTL только у головы очереди).
retry_queues = [
    RabbitQueue(
        f"payments.new.retry.{delay_ms // 1000}s",
        durable=True,
        arguments={
            "x-message-ttl": delay_ms,
            "x-dead-letter-exchange": payments_exchange.name,
            "x-dead-letter-routing-key": NEW_ROUTING_KEY,
        },
    )
    for delay_ms in RETRY_DELAYS_MS
]

dlq_queue = RabbitQueue("payments.new.dlq", durable=True, routing_key=DLQ_ROUTING_KEY)

# имена retry-очередей по индексу = номеру попытки (0 → 2s, 1 → 4s, 2 → 8s);
# consumer публикует в них напрямую через default exchange по имени очереди
RETRY_QUEUE_NAMES = tuple(q.name for q in retry_queues)


async def declare_topology() -> None:
    """Идемпотентно объявляет всю топологию (exchange'и, очереди, биндинги) в RabbitMQ.

    Вызывается один раз при старте consumer'а — RabbitBroker не делает этого
    сам за пределами @broker.subscriber, а нам нужны ещё retry-очереди и DLQ.
    """
    payments_exch = await broker.declare_exchange(payments_exchange)
    dlx_exch = await broker.declare_exchange(dlx_exchange)

    new_q = await broker.declare_queue(new_queue)
    await new_q.bind(payments_exch, routing_key=NEW_ROUTING_KEY)

    for retry_queue in retry_queues:
        # без биндинга к exchange — публикация идёт напрямую в очередь (см. комментарий выше)
        await broker.declare_queue(retry_queue)

    dlq_q = await broker.declare_queue(dlq_queue)
    await dlq_q.bind(dlx_exch, routing_key=DLQ_ROUTING_KEY)
