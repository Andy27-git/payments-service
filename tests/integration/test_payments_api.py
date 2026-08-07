from __future__ import annotations

import uuid

from sqlalchemy import select

from app.models import Outbox, Payment

API_KEY = "test-api-key"
AUTH_HEADERS = {"X-API-Key": API_KEY}

VALID_BODY = {
    "amount": "10.00",
    "currency": "RUB",
    "description": "test payment",
    "webhook_url": "http://example.com/hook",
}


async def test_create_payment_returns_202_pending_and_outbox_row(client, session_factory):
    response = await client.post(
        "/api/v1/payments",
        json=VALID_BODY,
        headers={**AUTH_HEADERS, "Idempotency-Key": "k-1"},
    )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "pending"
    payment_id = uuid.UUID(body["payment_id"])

    async with session_factory() as session:
        outbox_row = (await session.execute(select(Outbox))).scalar_one()
        assert outbox_row.payload == {"payment_id": str(payment_id)}


async def test_create_payment_repeat_same_key_does_not_duplicate(client, session_factory):
    headers = {**AUTH_HEADERS, "Idempotency-Key": "k-2"}

    first = await client.post("/api/v1/payments", json=VALID_BODY, headers=headers)
    second = await client.post("/api/v1/payments", json=VALID_BODY, headers=headers)

    assert first.status_code == 202
    assert second.status_code == 202
    assert first.json()["payment_id"] == second.json()["payment_id"]

    async with session_factory() as session:
        count = len((await session.execute(select(Payment))).scalars().all())
        assert count == 1


async def test_create_payment_repeat_same_key_different_body_returns_409(client):
    headers = {**AUTH_HEADERS, "Idempotency-Key": "k-3"}

    await client.post("/api/v1/payments", json=VALID_BODY, headers=headers)
    conflicting = {**VALID_BODY, "amount": "99.00"}
    response = await client.post("/api/v1/payments", json=conflicting, headers=headers)

    assert response.status_code == 409


async def test_create_payment_missing_idempotency_key_returns_422(client):
    response = await client.post("/api/v1/payments", json=VALID_BODY, headers=AUTH_HEADERS)

    assert response.status_code == 422


async def test_create_payment_missing_api_key_returns_401(client):
    # X-API-Key объявлен как Header(None) — отсутствие заголовка это ошибка
    # аутентификации (401), а не валидации запроса (422)
    response = await client.post(
        "/api/v1/payments", json=VALID_BODY, headers={"Idempotency-Key": "k-4"}
    )

    assert response.status_code == 401


async def test_create_payment_non_ascii_api_key_returns_401(client):
    # httpx.Headers сам кодирует str-заголовки как ascii и упал бы ещё до отправки
    # запроса; передаём заголовок как bytes — так же, как он приходит по проводу
    # (ASGI decodes header values as latin-1), чтобы воспроизвести реальный кейс
    response = await client.post(
        "/api/v1/payments",
        json=VALID_BODY,
        headers={"X-API-Key": "пароль".encode(), "Idempotency-Key": b"k-4b"},
    )

    assert response.status_code == 401


async def test_create_payment_wrong_api_key_returns_401(client):
    response = await client.post(
        "/api/v1/payments",
        json=VALID_BODY,
        headers={"X-API-Key": "wrong-key", "Idempotency-Key": "k-5"},
    )

    assert response.status_code == 401


async def test_create_payment_non_positive_amount_returns_422(client):
    body = {**VALID_BODY, "amount": "0"}
    response = await client.post(
        "/api/v1/payments", json=body, headers={**AUTH_HEADERS, "Idempotency-Key": "k-6"}
    )

    assert response.status_code == 422


async def test_create_payment_unknown_currency_returns_422(client):
    body = {**VALID_BODY, "currency": "JPY"}
    response = await client.post(
        "/api/v1/payments", json=body, headers={**AUTH_HEADERS, "Idempotency-Key": "k-7"}
    )

    assert response.status_code == 422


async def test_create_payment_invalid_webhook_url_returns_422(client):
    body = {**VALID_BODY, "webhook_url": "not-a-url"}
    response = await client.post(
        "/api/v1/payments", json=body, headers={**AUTH_HEADERS, "Idempotency-Key": "k-8"}
    )

    assert response.status_code == 422


async def test_get_payment_returns_404_for_unknown_id(client):
    response = await client.get(f"/api/v1/payments/{uuid.uuid4()}", headers=AUTH_HEADERS)

    assert response.status_code == 404


async def test_get_payment_returns_200_with_details(client):
    create_response = await client.post(
        "/api/v1/payments",
        json=VALID_BODY,
        headers={**AUTH_HEADERS, "Idempotency-Key": "k-9"},
    )
    payment_id = create_response.json()["payment_id"]

    response = await client.get(f"/api/v1/payments/{payment_id}", headers=AUTH_HEADERS)

    assert response.status_code == 200
    body = response.json()
    assert body["payment_id"] == payment_id
    assert body["status"] == "pending"
    assert body["webhook_attempts"] == 0
    assert body["webhook_delivered_at"] is None
