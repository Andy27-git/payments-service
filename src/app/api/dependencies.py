from __future__ import annotations

import secrets

from fastapi import Header, HTTPException, status

from app.config import get_settings
from app.db.session import get_session

__all__ = ["get_db_session", "verify_api_key"]

get_db_session = get_session


async def verify_api_key(x_api_key: str | None = Header(None)) -> None:
    # x_api_key: str | None — отсутствие заголовка тоже должно быть 401 (а не 422,
    # как было бы при Header(...) без дефолта): это ошибка аутентификации, а не
    # валидации запроса.
    # compare_digest вместо "==" — защита от timing-атак при сравнении секрета;
    # сравниваем байты, а не str — compare_digest падает с TypeError на строках
    # с не-ASCII символами.
    api_key = get_settings().api_key.encode("utf-8")
    if x_api_key is None or not secrets.compare_digest(x_api_key.encode("utf-8"), api_key):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")
