import logging
import socket
import time

import pytest
import structlog
from confluent_kafka import Consumer, Producer


@pytest.mark.parametrize("client_type", [Producer, Consumer])
def test_native_client_logs_connection_failure_with_structlog(caplog, client_type):
    logger = structlog.wrap_logger(
        logging.getLogger("kafka-native-forwarding-test"),
        wrapper_class=structlog.stdlib.BoundLogger,
        processors=[
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.KeyValueRenderer(),
        ],
    )
    # Reserve a local port without listening so connections are deterministically refused.
    with socket.socket() as endpoint:
        endpoint.bind(("127.0.0.1", 0))
        config = {
            "bootstrap.servers": f"127.0.0.1:{endpoint.getsockname()[1]}",
            "log.thread.name": False,
        }
        if client_type is Consumer:
            config["group.id"] = "test-logging"
        client = client_type(config, logger=logger)
        try:
            if client_type is Consumer:
                client.subscribe(["test-logging"])
            with caplog.at_level(logging.WARNING, logger="kafka-native-forwarding-test"):
                deadline = time.monotonic() + 3
                while time.monotonic() < deadline:
                    client.poll(0.1)
                    if any("Connection refused" in r.getMessage() for r in caplog.records):
                        break
        finally:
            if client_type is Consumer:
                client.close()

    failures = [r for r in caplog.records if "Connection refused" in r.getMessage()]
    assert failures
    assert all(r.levelno == logging.ERROR for r in failures)
