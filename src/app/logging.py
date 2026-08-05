from __future__ import annotations

import contextlib
import contextvars
import json
import logging
import sys
from collections.abc import Iterator
from typing import Any

# default=None (а не {}) — ruff B039 запрещает мутируемые дефолты ContextVar;
# отсутствие контекста всегда трактуем как пустой словарь через `or {}` ниже
_context: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar(
    "log_context", default=None
)

# поля "пустой" LogRecord — чтобы в JSON-вывод попадали только кастомные extra-поля
# (payment_id, retry_count и т.п.), а не служебные атрибуты самого LogRecord
_RESERVED_RECORD_KEYS = set(logging.LogRecord("", 0, "", 0, "", None, None).__dict__) | {"message"}


class _ContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        for key, value in (_context.get() or {}).items():
            setattr(record, key, value)
        return True


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED_RECORD_KEYS:
                payload[key] = value
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, ensure_ascii=False)


def setup_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    handler.addFilter(_ContextFilter())

    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)


@contextlib.contextmanager
def log_context(**fields: Any) -> Iterator[None]:
    """Добавляет поля (например payment_id, retry_count) во все логи внутри блока."""
    token = _context.set({**(_context.get() or {}), **fields})
    try:
        yield
    finally:
        _context.reset(token)
