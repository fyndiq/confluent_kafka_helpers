import time
from functools import wraps
from typing import Callable

import structlog

logger = structlog.get_logger(__name__)


def retry_exception(
    exceptions,
    retries=3,
    condition: Callable = lambda exc: True,
    backoff: float = 0,
    max_backoff: float = 10,
):
    """
    Retry the wrapped function when it raises one of `exceptions` and `condition(exc)` is true.

    `retries` is the total number of attempts. With `backoff` > 0 it sleeps `backoff` seconds
    before the first retry and doubles the sleep for each later retry, up to `max_backoff`.
    """

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
                            if backoff > 0:
                                time.sleep(min(backoff * 2 ** (retry_count - 1), max_backoff))
                            continue
                    raise

        return wrapped

    return decorator
