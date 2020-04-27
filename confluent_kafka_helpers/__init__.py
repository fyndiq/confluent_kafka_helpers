"""
Default atexit registered functions will not be triggered on error signals.

However - if we manually handle the error signals and exit, the functions will
be triggered.
"""
import signal
import sys

import confluent_kafka
import structlog

logger = structlog.get_logger(__name__)
logger.debug(
    "Using confluent kafka versions", version=confluent_kafka.version(),
    libversion=confluent_kafka.libversion()
)


def error_handler(signum, frame):
    logger.info("Received signal", signum=signum)
    exit_code = 128 + signum
    sys.exit(exit_code)


def interrupt_handler(signum, frame):
    logger.info("Received signal", signum=signum)
    sys.exit(1)


signal.signal(signal.SIGTERM, error_handler)  # graceful shutdown
signal.signal(signal.SIGINT, interrupt_handler)  # keyboard interrupt
