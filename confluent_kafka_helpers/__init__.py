"""
Default exit functions (atexit) will not be triggered on signals.

However - if we manually register handlers for the signals the exit functions
are triggered correctly.

NOTE! Exit functions will not be triggered on SIGKILL, SIGSTOP or os._exit()
"""
import signal

import confluent_kafka
import structlog

logger = structlog.get_logger(__name__)
logger.debug(
    "Using confluent kafka versions", version=confluent_kafka.version(),
    libversion=confluent_kafka.libversion()
)


def error_handler(signum, frame):
    logger.debug("Received signal", signum=signum)


def interrupt_handler(signum, frame):
    logger.debug("Received signal", signum=signum)


signal.signal(signal.SIGTERM, error_handler)  # graceful shutdown
signal.signal(signal.SIGINT, interrupt_handler)  # keyboard interrupt
