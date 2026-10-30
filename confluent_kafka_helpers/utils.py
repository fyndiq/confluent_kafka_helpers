import time
from functools import wraps
from typing import Callable

import structlog
from confluent_kafka import KafkaError

logger = structlog.get_logger(__name__)


def is_retryable_commit_error(error: KafkaError) -> bool:
    return not error.fatal() and error.code() in {
        KafkaError.REQUEST_TIMED_OUT,
        KafkaError.BROKER_NOT_AVAILABLE,
        KafkaError.COORDINATOR_LOAD_IN_PROGRESS,
        KafkaError.NOT_COORDINATOR,
    }


def is_lost_assignment(error: KafkaError) -> bool:
    return not error.fatal() and error.code() in {
        KafkaError.UNKNOWN_MEMBER_ID,
        KafkaError._ASSIGNMENT_LOST,
    }


def retry_exception(exceptions, retries=3, condition: Callable = lambda exc: True, delay=0):
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
                            if delay:
                                time.sleep(delay * 2 ** (retry_count - 1))
                            continue
                    raise

        return wrapped

    return decorator
