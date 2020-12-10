from contextlib import ExitStack
from unittest.mock import MagicMock, Mock, patch

import pytest
from confluent_kafka import KafkaError as ConfluentKafkaError
from confluent_kafka import KafkaException

from confluent_kafka_helpers import loader
from confluent_kafka_helpers.exceptions import EndOfPartition, KafkaTransportError

from tests import config
from tests.kafka import KafkaError


@pytest.fixture
def avro_message_loader(confluent_avro_consumer, avro_schema_registry):
    loader_config = config.Config.KAFKA_REPOSITORY_LOADER_CONFIG
    with ExitStack() as stack:
        stack.enter_context(
            patch('confluent_kafka_helpers.loader.AvroLazyConsumer', confluent_avro_consumer)
        )
        yield loader.AvroMessageLoader(loader_config)


def test_avro_message_loader_init(
    confluent_avro_consumer, avro_message_loader, avro_schema_registry
):
    assert avro_message_loader.topic == 'a'
    assert avro_message_loader.num_partitions == 10
    assert confluent_avro_consumer.call_count == 1
    assert avro_schema_registry.call_count == 1


@pytest.mark.parametrize(
    'key, num_partitions, expected_response', [(b'90', 100, 65), (b'15', 10, 8)]
)
def test_default_partitioner(key, num_partitions, expected_response):
    """
    Test the default partitioner with different parameters
    """
    response = loader.default_partitioner(key, num_partitions)
    assert expected_response == response


def test_avro_message_loader_load(confluent_message, confluent_avro_consumer, avro_message_loader):
    partitioner = MagicMock(return_value=1)
    avro_message_loader.key_serializer = lambda arg: arg
    message_generator = avro_message_loader.load(key=1, partitioner=partitioner)
    message = next(message_generator)
    assert message.value == b'foobar'
    with pytest.raises(RuntimeError):
        message = next(message_generator)


class TestFindDuplicatedMessages:
    def test_should_log_duplicated_messages(self):
        message = Mock()
        message.value.return_value = 'a'
        message.key.return_value = '1'
        message.partition.return_value = '0'
        message.offset.return_value = '0'
        message.topic.return_value = 'f'
        message.topic.return_value = 'f'
        message.timestamp.return_value = 1517389192

        messages = [message, message]
        logger = Mock()
        loader.find_duplicated_messages(messages, logger)

        assert logger.critical.call_count == 1


class TestErrorHandler:
    def test_raises_endofpartition_on_kafkaerror_partition_eof(self):
        error = KafkaError(_code=ConfluentKafkaError._PARTITION_EOF)
        with pytest.raises(EndOfPartition):
            loader.default_error_handler(error)

    def test_raises_kafkatransporterror_on_kafkaerror_transport(self):
        error = KafkaError(_code=ConfluentKafkaError._TRANSPORT)
        with pytest.raises(KafkaTransportError):
            loader.default_error_handler(error)

    @pytest.mark.parametrize(
        'code',
        [
            (ConfluentKafkaError._ALL_BROKERS_DOWN),
            (ConfluentKafkaError._NO_OFFSET),
            (ConfluentKafkaError._TIMED_OUT),
        ],
    )
    def test_raises_kafkaexception_on_other_errors(self, code):
        error = KafkaError(_code=code)
        with pytest.raises(KafkaException):
            loader.default_error_handler(error)
