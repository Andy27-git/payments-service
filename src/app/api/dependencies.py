from __future__ import annotations

import secrets

from fastapi import Header, HTTPException, status

from app.config import get_settings
from app.db.session import get_session

__all__ = ["get_db_session", "verify_api_key"]

get_db_session = get_session


async def verify_api_key(x_api_key: str = Header(...)) -> None:
    # compare_digest вместо "==" — защита от timing-атак при сравнении секрета
    if not secrets.compare_digest(x_api_key, get_settings().api_key):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")
