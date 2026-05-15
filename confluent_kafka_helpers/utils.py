from functools import wraps
from typing import Callable

import structlog

logger = structlog.get_logger(__name__)


def retry_exception(exceptions, retries=3, condition: Callable = lambda exc: True):
    def decorator(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            retry_count = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as exc:
                    if any([isinstance(exc, e) for e in exceptions]) and condition(exc):
                        logger.warning("Retrying exception", exc=exc, retry=retry_count)
                        retry_count += 1
                        if retry_count < retries:
                            continue
                    raise

        return wrapped

    return decorator
