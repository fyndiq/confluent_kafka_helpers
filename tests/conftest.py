from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka.avro import AvroConsumer as ConfluentAvroConsumer

from confluent_kafka_helpers import consumer

from tests import config


@pytest.fixture
def confluent_message():
    def _make_message(headers=None):
        class PollReturnMock:
            def __init__(self):
                self.value = MagicMock(return_value=b"foobar")
                self.error = MagicMock(return_value=None)
                self.key = MagicMock(return_value=1)
                self.offset = MagicMock(return_value=0)
                self.partition = MagicMock(return_value=1)
                self.topic = MagicMock(return_value="test")
                self.timestamp = MagicMock(return_value=(1, -1))
                self.headers = MagicMock(return_value=headers or [("foo", b"bar")])

        return PollReturnMock()

    return _make_message


@pytest.fixture
def confluent_avro_consumer(confluent_message):
    def _create_consumer(message=None):
        class ConfluentAvroConsumerMock(MagicMock):
            subscribe = MagicMock()
            poll = MagicMock(
                name="poll", side_effect=[message or confluent_message(), StopIteration]
            )
            close = MagicMock()
            get_watermark_offsets = MagicMock(name="watermark_test", return_value=[1, 1])

        return ConfluentAvroConsumerMock(
            spec=ConfluentAvroConsumer, name="ConfluentAvroConsumerMock"
        )

    return _create_consumer


@pytest.fixture
def avro_consumer(confluent_avro_consumer, confluent_message):
    def _create_consumer(config_override=None, headers=None):
        config_data = config.Config.KAFKA_CONSUMER_CONFIG.copy()
        config_data["client.id"] = "<client-id>"
        if config_override:
            config_data.update(config_override)

        message = confluent_message(headers=headers)
        mock_consumer = confluent_avro_consumer(message=message)

        with ExitStack() as stack:
            stack.enter_context(
                patch(
                    "confluent_kafka_helpers.consumer.ConfluentAvroConsumer",
                    mock_consumer,
                )
            )
            avro_consumer = consumer.AvroConsumer(config_data)
            setattr(avro_consumer, "_mock_consumer", mock_consumer)
            return avro_consumer

    return _create_consumer


@pytest.fixture
def avro_schema_registry():
    schema_registry = MagicMock()
    with ExitStack() as stack:
        stack.enter_context(
            patch("confluent_kafka_helpers.loader.AvroSchemaRegistry", schema_registry)
        )
        yield schema_registry
