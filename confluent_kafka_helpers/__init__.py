"""
Default exit functions (atexit) will not be triggered on signals.

However - if we manually register handlers and `sys.exit()` the exit functions
are triggered correctly.

NOTE! Exit functions will not be triggered on SIGKILL, SIGSTOP or os._exit()
"""

import signal
import threading

import structlog

logger = structlog.get_logger(__name__)
existing_termination_handler = signal.getsignal(signal.SIGTERM)
existing_interrupt_handler = signal.getsignal(signal.SIGINT)

shutdown_requested = threading.Event()


def termination_handler(signum, frame):
    logger.info("Received termination signal", signum=signum)
    shutdown_requested.set()
    if existing_termination_handler:
        logger.debug(
            "Using existing termination handler",
            name=existing_termination_handler.__qualname__,
        )
        existing_termination_handler(signum, frame)


def interrupt_handler(signum, frame):
    logger.info("Received interrupt signal", signum=signum)
    shutdown_requested.set()
    if existing_interrupt_handler:
        logger.debug(
            "Using existing interrupt handler",
            name=existing_interrupt_handler.__qualname__,
        )
        existing_interrupt_handler(signum, frame)


signal.signal(signal.SIGTERM, termination_handler)
signal.signal(signal.SIGINT, interrupt_handler)
