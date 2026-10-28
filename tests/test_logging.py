import logging
import socket
import time

import pytest
import structlog
from confluent_kafka import Consumer, Producer

from confluent_kafka_helpers.logging import KafkaLogger


@pytest.mark.parametrize(
    "facility,message,level,expected_level",
    [
        (
            "FAIL",
            "sasl_ssl://192.0.2.1:9092/9: Connect to ipv4#192.0.2.1:9092 "
            "failed: Connection refused (after 1ms in state CONNECT)",
            logging.ERROR,
            logging.WARNING,
        ),
        (
            "FAIL",
            "[thrd:broker:9092/1]: broker:9092/1: Disconnected (after 100ms in state UP)",
            logging.ERROR,
            logging.WARNING,
        ),
        (
            "FAIL",
            "[thrd:broker:9092/1]: broker:9092/1: Connect to ipv4#127.0.0.1:9092 "
            "failed: Connection refused (after 0ms in state CONNECT)",
            logging.ERROR,
            logging.WARNING,
        ),
        (
            "FAIL",
            "[thrd:broker:9092/1]: broker:9092/1: Failed to resolve 'broker:9092': "
            "Name or service not known (after 1ms in state CONNECT)",
            logging.ERROR,
            logging.WARNING,
        ),
        ("FAIL", "SASL authentication failed", logging.ERROR, logging.ERROR),
        ("FAIL", "SSL handshake failed", logging.ERROR, logging.ERROR),
        ("FAIL", "Unknown topic", logging.ERROR, logging.ERROR),
        ("FAIL", "Disconnected unexpectedly", logging.ERROR, logging.ERROR),
        ("ERROR", "Connection refused", logging.ERROR, logging.ERROR),
        ("FAIL", "Fatal client failure", logging.CRITICAL, logging.CRITICAL),
        ("INFO", "Connected", logging.INFO, logging.INFO),
    ],
)
def test_forwards_real_stdlib_structlog(caplog, facility, message, level, expected_level):
    logger = structlog.wrap_logger(
        logging.getLogger("kafka-forwarding-test"),
        wrapper_class=structlog.stdlib.BoundLogger,
        processors=[
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.KeyValueRenderer(),
        ],
    )
    with caplog.at_level(logging.DEBUG, logger="kafka-forwarding-test"):
        KafkaLogger(logger).log(level, "%s [%s] %s", facility, "client#consumer-1", message)

    (record,) = caplog.records
    assert record.levelno == expected_level
    assert f"{facility} [client#consumer-1] {message}" in record.getMessage()


def test_works_with_filtering_structlog_logger():
    sink = structlog.testing.ReturnLogger()
    logger = structlog.wrap_logger(
        sink,
        wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG),
        processors=[],
    )
    KafkaLogger(logger).log(logging.ERROR, "%s [%s] %s", "FAIL", "client", "Unknown error")


@pytest.mark.parametrize("client_type", [Producer, Consumer])
def test_native_client_reconnect_log_with_default_thread_setting(caplog, client_type):
    logger = structlog.wrap_logger(
        logging.getLogger("kafka-native-forwarding-test"),
        wrapper_class=structlog.stdlib.BoundLogger,
        processors=[structlog.processors.KeyValueRenderer()],
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
        client = client_type(config, logger=KafkaLogger(logger))
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
    assert all(r.levelno == logging.WARNING for r in failures)
