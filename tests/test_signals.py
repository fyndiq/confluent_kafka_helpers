"""
Contract tests for graceful shutdown on SIGTERM / SIGINT.

Motivation (PADE-617): when Kubernetes scales down a pod, the kubelet sends
SIGTERM to PID 1 in each container. The current handler in
`confluent_kafka_helpers/__init__.py` reacts by calling `sys.exit(0)`, which
raises `SystemExit` at the next bytecode boundary on the main thread. If the
signal arrives in the middle of a Kafka message handler (between a DB commit
and an external HTTP call, say), the handler is interrupted mid-flight and the
application's `except Exception:` cleanup wrapper does not catch it
(`SystemExit` inherits from `BaseException`, not `Exception`).

The desired behaviour is "drain-on-shutdown":

* First SIGTERM / SIGINT must NOT raise — it just records that shutdown was
  requested. In-flight Python code (the user's message handler) runs to
  completion.
* The consumer loop checks the flag between messages and exits cleanly.
* A second signal is treated as an operator escape hatch: "I really mean it,
  exit now" — that one is allowed to raise `SystemExit`.

The flag itself is exposed as `confluent_kafka_helpers.shutdown_requested` so
that user code (and the consumer loop) can poll it.
"""

import signal
import threading

import pytest

import confluent_kafka_helpers


@pytest.fixture(autouse=True)
def _reset_shutdown_state():
    """Clear the module-level shutdown flag before and after every test.
    Without this, a test that sets the flag leaks state into the next test."""
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
        assert isinstance(
            confluent_kafka_helpers.shutdown_requested, threading.Event
        ), (
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

    def test_second_signal_raises_systemexit(self, isolated_handlers):
        """Operator escape hatch: a second SIGTERM means 'I really mean it'.
        The handler is allowed to short-circuit and exit immediately."""
        confluent_kafka_helpers.termination_handler(signal.SIGTERM, None)
        with pytest.raises(SystemExit):
            confluent_kafka_helpers.termination_handler(signal.SIGTERM, None)


class TestInterruptHandler:
    def test_first_signal_does_not_raise(self, isolated_handlers):
        confluent_kafka_helpers.interrupt_handler(signal.SIGINT, None)

    def test_first_signal_sets_shutdown_flag(self, isolated_handlers):
        confluent_kafka_helpers.interrupt_handler(signal.SIGINT, None)
        assert confluent_kafka_helpers.shutdown_requested.is_set()

    def test_second_signal_raises_systemexit(self, isolated_handlers):
        confluent_kafka_helpers.interrupt_handler(signal.SIGINT, None)
        with pytest.raises(SystemExit):
            confluent_kafka_helpers.interrupt_handler(signal.SIGINT, None)


class TestSignalsShareTheSameFlag:
    def test_sigterm_sets_the_same_flag_sigint_does(self, isolated_handlers):
        confluent_kafka_helpers.termination_handler(signal.SIGTERM, None)
        assert confluent_kafka_helpers.shutdown_requested.is_set()

    def test_sigint_sets_the_same_flag_sigterm_does(self, isolated_handlers):
        confluent_kafka_helpers.interrupt_handler(signal.SIGINT, None)
        assert confluent_kafka_helpers.shutdown_requested.is_set()
