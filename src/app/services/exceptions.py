class IdempotencyConflictError(Exception):
    """Тот же Idempotency-Key уже использован с другим телом запроса."""
