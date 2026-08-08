from __future__ import annotations

AUTH_HEADERS = {"X-API-Key": "test-api-key"}


async def test_health_requires_api_key(client):
    # ТЗ требует X-API-Key для всех эндпоинтов — /health не исключение
    response = await client.get("/health")

    assert response.status_code == 401


async def test_health_returns_ok_with_api_key(client):
    response = await client.get("/health", headers=AUTH_HEADERS)

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
