import time
from functools import wraps

import structlog

logger = structlog.get_logger(__name__)


def retry_exception(exceptions, retries=3, delay=0):
    def decorator(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            retry_count = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as exc:
                    if any([isinstance(exc, e) for e in exceptions]):
                        logger.info(
                            "Retrying exception", exc=exc, retry=retry_count,
                            delay=delay
                        )
                        time.sleep(delay)
                        retry_count += 1
                        if retry_count < retries:
                            continue
                    raise

        return wrapped

    return decorator
