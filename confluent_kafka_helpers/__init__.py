"""
Default exit functions (atexit) will not be triggered on signals.

However - if we manually register handlers and `sys.exit()` the exit functions
are triggered correctly.

NOTE! Exit functions will not be triggered on SIGKILL, SIGSTOP or os._exit()
"""
import signal
import sys

import confluent_kafka
import structlog

logger = structlog.get_logger(__name__)
logger.debug(
    "Using confluent kafka versions",
    version=confluent_kafka.version(),
    libversion=confluent_kafka.libversion(),
)

existing_termination_handler = signal.getsignal(signal.SIGTERM)
existing_interrupt_handler = signal.getsignal(signal.SIGINT)


def termination_handler(signum, frame):
    logger.debug("Received termination signal", signum=signum)
    if existing_termination_handler:
        logger.debug(
            "Using existing termination handler", name=existing_termination_handler.__qualname__
        )
        existing_termination_handler(signum, frame)
    else:
        sys.exit(0)


def interrupt_handler(signum, frame):
    logger.debug("Received interrupt signal", signum=signum)
    if existing_interrupt_handler is not signal.default_int_handler:
        logger.debug(
            "Using existing interrupt handler", name=existing_interrupt_handler.__qualname__
        )
        existing_interrupt_handler(signum, frame)
    else:
        sys.exit(0)


signal.signal(signal.SIGTERM, termination_handler)
signal.signal(signal.SIGINT, interrupt_handler)
