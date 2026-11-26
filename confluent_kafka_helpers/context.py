import contextvars
from contextlib import contextmanager
from typing import Any

from opentelemetry.context import attach, detach

from confluent_kafka_helpers.message import Message
from confluent_kafka_helpers.tracing import tracer

_headers: contextvars.ContextVar[dict[str, Any]] = contextvars.ContextVar("headers", default={})


def set_propagated_headers(headers: dict[str, Any]):
    _headers.set(headers)


def get_propagated_headers() -> dict[str, Any]:
    return _headers.get()


def clear_propagated_headers():
    _headers.set({})


@contextmanager
def propagated_headers_context(headers: dict[str, Any]):
    token = _headers.set(headers)
    try:
        yield
    finally:
        _headers.reset(token)


@contextmanager
def message_context(message: Message):
    """Activate one input's headers and trace for a sequential handler."""
    with propagated_headers_context(dict(message.propagated_headers)):
        token = attach(tracer.extract_headers(headers=message._meta.headers))
        try:
            yield
        finally:
            detach(token)
