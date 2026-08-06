from __future__ import annotations

import httpx

from app.models import Payment

WEBHOOK_TIMEOUT_SECONDS = 5.0


async def send_webhook(payment: Payment) -> bool:
    """Одна HTTP-попытка доставки вебхука. True — доставлен, False — технический сбой (нужен retry)."""
    payload = {
        "payment_id": str(payment.id),
        "status": payment.status,
        "amount": str(payment.amount),
        "currency": payment.currency,
    }
    try:
        async with httpx.AsyncClient(timeout=WEBHOOK_TIMEOUT_SECONDS) as client:
            response = await client.post(payment.webhook_url, json=payload)
    except httpx.HTTPError:
        return False
    return response.is_success
