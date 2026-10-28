import logging
import re


class KafkaLogger:
    """Forward librdkafka's stdlib-style log calls to structlog."""

    _reconnect = re.compile(
        r"^(?:\[thrd:[^\]]+\]: )?.+: (?:"
        r"Disconnected \(after .* in state UP\)|"
        r"Connect to .* failed: (?:Connection refused|Connection timed out|"
        r"Network is unreachable)|"
        r"Failed to resolve .*: (?:Name or service not known|"
        r"Temporary failure in name resolution|nodename nor servname provided))"
    )

    def __init__(self, logger):
        self.logger = logger

    def log(self, level, message, *args, **kwargs):
        # librdkafka passes facility, client name and message as positional arguments.
        if (
            level == logging.ERROR
            and message == "%s [%s] %s"
            and len(args) == 3
            and args[0] == "FAIL"
            and self._reconnect.match(args[2])
        ):
            level = logging.WARNING
        self.logger.log(level, message % args if args else message, **kwargs)
