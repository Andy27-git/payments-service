from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import get_db_session, verify_api_key

# ТЗ требует X-API-Key "для всех эндпоинтов", поэтому /health тоже под ключом;
# docker healthcheck передаёт его из той же переменной API_KEY (см. docker-compose.yml)
router = APIRouter(dependencies=[Depends(verify_api_key)])


@router.get("/health")
async def health(session: AsyncSession = Depends(get_db_session)) -> dict[str, str]:
    await session.execute(text("SELECT 1"))
    return {"status": "ok"}
