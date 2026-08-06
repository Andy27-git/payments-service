from __future__ import annotations

import asyncio
import random

from app.models import PaymentStatus

SUCCESS_PROBABILITY = 0.9
MIN_DELAY_SECONDS = 2.0
MAX_DELAY_SECONDS = 5.0


async def process_payment() -> PaymentStatus:
    """Эмулирует обращение к внешнему платёжному шлюзу: задержка + случайный исход."""
    await asyncio.sleep(random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS))
    return PaymentStatus.SUCCEEDED if random.random() < SUCCESS_PROBABILITY else PaymentStatus.FAILED
