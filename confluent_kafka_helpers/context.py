import contextlib
import contextvars
from typing import Any

_headers: contextvars.ContextVar[dict[str, Any]] = contextvars.ContextVar("headers", default={})


def set_propagated_headers(headers: dict[str, Any]):
    _headers.set(headers)


def get_propagated_headers() -> dict[str, Any]:
    return _headers.get()


def clear_propagated_headers():
    _headers.set({})


@contextlib.contextmanager
def propagated_headers(headers: dict[str, Any]):
    """Set the propagated headers inside the block, restore the previous ones on exit."""
    token = _headers.set(headers)
    try:
        yield
    finally:
        _headers.reset(token)
