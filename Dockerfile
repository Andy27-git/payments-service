# syntax=docker/dockerfile:1
FROM python:3.12-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.9.23 /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never

WORKDIR /app

# зависимости ставятся отдельным слоем от кода приложения (кэш меняется только
# когда меняются pyproject.toml/uv.lock, а не при каждой правке src/)
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-dev


FROM python:3.12-slim AS runtime

RUN groupadd --system --gid 1000 app \
    && useradd --system --uid 1000 --gid app --home /app --shell /usr/sbin/nologin app

WORKDIR /app

COPY --from=builder --chown=app:app /app/.venv /app/.venv
COPY --chown=app:app src ./src
COPY --chown=app:app alembic ./alembic
COPY --chown=app:app alembic.ini ./

# src/ не устанавливается как пакет (tool.uv package = false, см. pyproject.toml),
# поэтому она попадает в sys.path через PYTHONPATH одинаково для uvicorn и consumer
ENV PATH="/app/.venv/bin:$PATH" PYTHONPATH="/app/src"

USER app

EXPOSE 8000

# дефолтная команда — api; consumer запускается тем же образом с переопределённой
# командой (см. docker-compose.yml). Без CMD образ был бы непригоден для docker run
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
