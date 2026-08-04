from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

from app.models.payment import Currency, PaymentStatus


class PaymentCreate(BaseModel):
    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: Currency
    description: str = Field(max_length=512)
    metadata: dict[str, Any] | None = None
    webhook_url: HttpUrl


class PaymentAccepted(BaseModel):
    """Ответ POST /api/v1/payments — ровно как в ТЗ."""

    model_config = ConfigDict(from_attributes=True)

    payment_id: uuid.UUID = Field(validation_alias="id")
    status: PaymentStatus
    created_at: datetime


class PaymentDetail(BaseModel):
    """Ответ GET /api/v1/payments/{payment_id} — полная информация о платеже."""

    model_config = ConfigDict(from_attributes=True)

    payment_id: uuid.UUID = Field(validation_alias="id")
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any] | None = Field(default=None, validation_alias="metadata_")
    status: PaymentStatus
    idempotency_key: str
    webhook_url: HttpUrl
    webhook_delivered_at: datetime | None
    webhook_attempts: int
    created_at: datetime
    processed_at: datetime | None
