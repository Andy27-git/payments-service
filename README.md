# Payments Service

Асинхронный сервис процессинга платежей: принимает запрос на оплату, публикует
событие в RabbitMQ через outbox pattern, обрабатывает его consumer'ом (эмуляция
платёжного шлюза) и уведомляет клиента через webhook — с повторными попытками
и Dead Letter Queue при окончательном сбое доставки.

## Стек

FastAPI + Pydantic v2 · SQLAlchemy 2.0 (async) · PostgreSQL · RabbitMQ (FastStream) ·
Alembic · Docker / docker-compose

## Запуск

```bash
git clone <repo-url> && cd payments-service
cp .env.example .env
docker compose up --build
```

Поднимаются 4 сервиса: `postgres`, `rabbitmq`, `api` (гоняет `alembic upgrade head`,
затем `uvicorn`), `consumer` (FastStream-приложение с обработчиком `payments.new` и
фоновой outbox-relay задачей). Порядок старта: `postgres` + `rabbitmq` (healthy) →
`api` (миграции + healthy) → `consumer`.

- API: `http://localhost:8000` (`GET /health` — без авторизации)
- RabbitMQ management UI: `http://localhost:15672` (guest/guest)

## API

Все эндпоинты `/api/v1/*` требуют заголовок `X-API-Key` (значение — `API_KEY` из `.env`).

### `POST /api/v1/payments`

Обязательный заголовок `Idempotency-Key`.

```bash
curl -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: change-me" \
  -H "Idempotency-Key: order-42" \
  -H "Content-Type: application/json" \
  -d '{
        "amount": "10.00",
        "currency": "RUB",
        "description": "тестовый платёж",
        "metadata": {"order_id": "42"},
        "webhook_url": "https://webhook.site/<your-id>"
      }'
# 202 Accepted
# {"payment_id": "...", "status": "pending", "created_at": "..."}
```

- Повтор того же запроса с тем же `Idempotency-Key` и тем же телом → тот же
  `payment_id`, без дублирования в БД (тоже 202).
- Тот же `Idempotency-Key`, но **другое** тело → `409 Conflict`.
- Без `X-API-Key` или без `Idempotency-Key` → `422` (оба объявлены как обязательные
  заголовки — FastAPI отклоняет запрос на этапе валидации параметров, до вызова
  какой-либо бизнес-логики). Заголовок `X-API-Key` присутствует, но неверный → `401`.
- Некорректное тело (`amount <= 0`, неизвестная валюта, невалидный `webhook_url`) → `422`.

### `GET /api/v1/payments/{payment_id}`

```bash
curl http://localhost:8000/api/v1/payments/<payment_id> -H "X-API-Key: change-me"
```

Возвращает полную информацию о платеже, включая `status`, `webhook_attempts`,
`webhook_delivered_at`, `processed_at`. Неизвестный `payment_id` → `404`.

## Поток обработки

```
POST /api/v1/payments
        │  одна транзакция: INSERT payments(status=pending) + INSERT outbox
        ▼
   таблица outbox
        │  outbox relay (фоновая задача в consumer, опрос ~0.5с,
        │  SELECT ... FOR UPDATE SKIP LOCKED)
        ▼
   RabbitMQ: exchange "payments" → queue payments.new
        │
        ▼
   consumer: handle_payment_new  ◄────────────────────────────┐
        │                                                      │
        ├─ status == pending? ─── да ──► gateway-эмулятор       │
        │                               (2-5с, 90% succeeded /  │
        │                                10% failed)            │
        │                                     │                 │
        │                                     ▼                 │
        └─ status уже выставлен ───────► webhook/sender.py       │ TTL истёк →
          (повторная доставка,           (1 попытка)             │ назад в payments.new
           см. "Идемпотентность                                  │ (x-dead-letter-*)
           обработчика" ниже)                 │                  │
                                  ┌─────────────┴──────────────┐  │
                                  ▼                             ▼  │
                          2xx: webhook_delivered_at    техническая ошибка
                          проставлен, сообщение        (5xx/timeout/БД недоступна)
                          ack'ается                             │
                                                    x-retry-count < 3?
                                                     да │           │ нет
                                             publish в  │           │ publish в payments.dlx
                                    payments.new.retry.{2s|4s|8s}   ▼
                                                                queue payments.new.dlq
```

Исходное сообщение из `payments.new` **всегда ack'ается** — повтор организован не
через `nack`/requeue брокера, а явной публикацией в отдельные retry-очереди с
`x-message-ttl` (2000/4000/8000 мс) и `x-dead-letter-exchange: payments,
x-dead-letter-routing-key: payments.new`: по истечении TTL RabbitMQ сам
перекладывает сообщение обратно в `payments.new`.

### Бизнес-провал ≠ технический сбой

10% «ошибок» от gateway-эмулятора — это **терминальный** результат обработки
(`status=failed`): webhook с этим результатом отправляется нормально, в retry/DLQ
сообщение не уходит. Retry-механизм предназначен только для сбоев доставки —
недоступной БД, таймаута или 5xx от `webhook_url`. Смешивать эти два случая
означало бы либо ретраить платежи, которые уже честно завершились неуспехом, либо
никогда не уведомлять клиента об инфраструктурных сбоях.

### Идемпотентность обработчика

Сообщение может быть доставлено повторно (redelivery из retry-очереди после
неудачного вебхука, дубль от at-least-once publish в outbox relay). Обработчик
выходит без действий, только если платёж **и** терминален (`status != pending`),
**и** вебхук уже доставлен (`webhook_delivered_at is not None`). Проверка одного
статуса была бы ошибочной: сообщение, вернувшееся из retry-очереди именно из-за
неудачного вебхука, увидело бы уже проставленный `status` и вышло бы, так и не
переотправив вебхук.

### Почему outbox relay живёт в процессе consumer'а

По ТЗ в docker-compose ровно 4 сервиса: `postgres`, `rabbitmq`, `api`, `consumer`.
Отдельного relay-сервиса нет — outbox relay запускается как фоновая asyncio-задача
внутри `consumer`, а не внутри `api`: тогда падение/перезапуск `api` не тормозит
публикацию накопленных событий.

## RabbitMQ топология

Все exchange и очереди — `durable=True`, все сообщения публикуются persistent
(`delivery_mode=2`), иначе рестарт контейнера `rabbitmq` терял бы всё, что outbox
pattern уже гарантированно доставил в брокер.

| Объект | Тип | Комментарий |
|---|---|---|
| `payments` | exchange (direct) | основной поток событий |
| `payments.new` | queue | обрабатывается consumer'ом |
| `payments.new.retry.2s` / `.4s` / `.8s` | queue | `x-message-ttl` + dead-letter обратно в `payments.new` |
| `payments.dlx` | exchange (direct) | публикуется явно из consumer'а после 3 неудачных попыток |
| `payments.new.dlq` | queue | конечная точка для сообщений, не обработанных после 3 попыток |

Три отдельные retry-очереди вместо одной с per-message `expiration` — потому что
RabbitMQ проверяет истечение TTL только у сообщения в **голове** очереди: с общим
backoff 2/4/8с сообщение с меньшим TTL ждало бы позади сообщения с большим TTL
столько же, сколько и оно (head-of-line blocking).

## Тесты

```bash
uv sync
uv run pytest              # unit + integration, sqlite in-memory, без Docker
uv run pytest -m pg         # + один тест на реальном Postgres (testcontainers)
```

`SELECT ... FOR UPDATE SKIP LOCKED` в sqlite не поддерживается по-настоящему
(конструкция не отклоняется, но и не даёт нужной семантики пропуска залоченных
строк) — как и нативный `JSONB`, и alembic-миграции против sqlite не прогонялись.
Поэтому конкурентное поведение outbox relay (двух relay-корутин, разбирающих одни
и те же строки без дублей) проверяется отдельным тестом с меткой `@pytest.mark.pg`:
поднимает Postgres через testcontainers, реально прогоняет `alembic upgrade head`
и только затем гоняет relay. Требует Docker; если он недоступен — тест
пропускается, а не падает.

## Переменные окружения

См. `.env.example`:

| Переменная | Назначение |
|---|---|
| `DATABASE_URL` | `postgresql+asyncpg://...` |
| `RABBITMQ_URL` | `amqp://...` |
| `API_KEY` | статический ключ для заголовка `X-API-Key` |
| `LOG_LEVEL` | уровень логирования (JSON-логи в stdout) |

## Проверка retry/DLQ вручную

1. Создать платёж с заведомо нерабочим `webhook_url` (например `http://127.0.0.1:1/hook`).
2. В RabbitMQ management UI (`http://localhost:15672`, очереди vhost `/`) видно, как
   сообщение проходит `payments.new.retry.2s` → `.4s` → `.8s`.
3. После третьей неудачной попытки сообщение оказывается в `payments.new.dlq`.
4. `GET /api/v1/payments/{id}` — `webhook_attempts = 3`, `webhook_delivered_at = null`.
