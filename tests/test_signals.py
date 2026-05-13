"""
Contract tests for graceful shutdown on SIGTERM / SIGINT.
The desired behaviour is "stop-after-current-message":

* SIGTERM / SIGINT must NOT raise — it just records that shutdown was
  requested. In-flight Python code (the user's message handler) runs to
  completion.
* The consumer loop checks the flag between messages and exits cleanly.

The flag itself is exposed as `confluent_kafka_helpers.shutdown_requested` so
that user code (and the consumer loop) can poll it.
"""

import signal
import threading
from unittest.mock import MagicMock

import pytest

import confluent_kafka_helpers


@pytest.fixture(autouse=True)
def _reset_shutdown_state():
    """Clear the module-level shutdown flag before and after every test."""
    flag = getattr(confluent_kafka_helpers, "shutdown_requested", None)
    if flag is not None:
        flag.clear()
    yield
    flag = getattr(confluent_kafka_helpers, "shutdown_requested", None)
    if flag is not None:
        flag.clear()


@pytest.fixture
def isolated_handlers(monkeypatch):
    """Strip pre-existing SIGTERM/SIGINT handlers captured at module import
    time (e.g. pytest's own SIGINT handler). Without this, calls to the
    library handlers chain into pytest internals and the tests become
    non-deterministic depending on import order."""
    for name in ("existing_termination_handler", "existing_interrupt_handler"):
        if hasattr(confluent_kafka_helpers, name):
            monkeypatch.setattr(f"confluent_kafka_helpers.{name}", None)


class TestShutdownRequestedFlag:
    def test_module_exposes_shutdown_requested_event(self):
        assert hasattr(confluent_kafka_helpers, "shutdown_requested"), (
            "confluent_kafka_helpers must expose a public `shutdown_requested` "
            "flag so the consume loop can check it between messages."
        )
        assert isinstance(confluent_kafka_helpers.shutdown_requested, threading.Event), (
            "`shutdown_requested` should be a `threading.Event` so it is "
            "thread-safe and has the standard is_set/set/clear API."
        )

    def test_shutdown_requested_is_initially_unset(self):
        assert not confluent_kafka_helpers.shutdown_requested.is_set()


class TestTerminationHandler:
    def test_first_signal_does_not_raise(self, isolated_handlers):
        confluent_kafka_helpers.termination_handler(signal.SIGTERM, None)

    def test_first_signal_sets_shutdown_flag(self, isolated_handlers):
        confluent_kafka_helpers.termination_handler(signal.SIGTERM, None)
        assert confluent_kafka_helpers.shutdown_requested.is_set()


class TestInterruptHandler:
    def test_first_signal_does_not_raise(self, isolated_handlers):
        confluent_kafka_helpers.interrupt_handler(signal.SIGINT, None)

    def test_first_signal_sets_shutdown_flag(self, isolated_handlers):
        confluent_kafka_helpers.interrupt_handler(signal.SIGINT, None)
        assert confluent_kafka_helpers.shutdown_requested.is_set()


class TestSignalsShareTheSameFlag:
    def test_sigterm_sets_the_same_flag_sigint_does(self, isolated_handlers):
        confluent_kafka_helpers.termination_handler(signal.SIGTERM, None)
        assert confluent_kafka_helpers.shutdown_requested.is_set()

    def test_sigint_sets_the_same_flag_sigterm_does(self, isolated_handlers):
        confluent_kafka_helpers.interrupt_handler(signal.SIGINT, None)
        assert confluent_kafka_helpers.shutdown_requested.is_set()


class TestChainedHandlers:
    """Regression tests for the chained-handler case.

    When this library is imported AFTER another framework (ddtrace, OTel SDK,
    gunicorn, uvicorn, ...) has already installed a SIGTERM/SIGINT handler,
    that handler is captured in `existing_*_handler` at import time and the
    library's handler chains to it.

    Both the chained handler AND the `shutdown_requested` flag must run.
    If we only chain and skip the flag, the consumer loop never sees the
    shutdown request and graceful shutdown silently breaks in any service
    that pulls in another signal-handling library — which is the common case
    in production.
    """

    @staticmethod
    def _make_handler_mock(qualname):
        # `__qualname__` is read for the debug log line; give it a real string
        # so structlog doesn't serialise a nested Mock.
        handler = MagicMock()
        handler.__qualname__ = qualname
        return handler

    def test_termination_calls_existing_handler_and_sets_flag(self, monkeypatch):
        existing = self._make_handler_mock("existing_sigterm")
        monkeypatch.setattr("confluent_kafka_helpers.existing_termination_handler", existing)

        confluent_kafka_helpers.termination_handler(signal.SIGTERM, None)

        existing.assert_called_once_with(signal.SIGTERM, None)
        assert confluent_kafka_helpers.shutdown_requested.is_set()

    def test_interrupt_calls_existing_handler_and_sets_flag(self, monkeypatch):
        existing = self._make_handler_mock("existing_sigint")
        monkeypatch.setattr("confluent_kafka_helpers.existing_interrupt_handler", existing)

        confluent_kafka_helpers.interrupt_handler(signal.SIGINT, None)

        existing.assert_called_once_with(signal.SIGINT, None)
        assert confluent_kafka_helpers.shutdown_requested.is_set()

    def test_flag_is_set_before_existing_handler_runs(self, monkeypatch):
        """Order matters: if the chained handler raises (e.g. an aggressive
        framework calls sys.exit), we still want the consumer loop to know
        shutdown was requested. So the flag must be set BEFORE chaining."""
        observed = {}

        def existing(signum, frame):
            observed["flag_was_set"] = confluent_kafka_helpers.shutdown_requested.is_set()

        existing.__qualname__ = "existing_sigterm"
        monkeypatch.setattr("confluent_kafka_helpers.existing_termination_handler", existing)

        confluent_kafka_helpers.termination_handler(signal.SIGTERM, None)

        assert observed["flag_was_set"] is True
