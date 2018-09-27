from unittest.mock import MagicMock, Mock, patch

import pytest
from confluent_kafka import KafkaError as ConfluentKafkaError
from confluent_kafka import KafkaException

from confluent_kafka_helpers import loader
from confluent_kafka_helpers.exceptions import (
    EndOfPartition, KafkaTransportError
)

from tests import config, conftest
from tests.kafka import KafkaError

mock_avro_consumer = conftest.ConfluentAvroConsumerMock(
    name='ConfluentAvroConsumerMock'
)
mock_avro_schema_registry = MagicMock()
mock_confluent_avro_consumer = conftest.mock_confluent_avro_consumer
mock_topic_partition = MagicMock()
mock_topic_partition.return_value = 1
mock_partitioner = MagicMock()
mock_partitioner.return_value = 1


@pytest.fixture(scope='function')
@patch('confluent_kafka_helpers.loader.AvroLazyConsumer', mock_avro_consumer)
@patch(
    'confluent_kafka_helpers.loader.AvroSchemaRegistry',
    mock_avro_schema_registry()
)
def avro_message_loader(avro_consumer):
    loader_config = config.Config.KAFKA_REPOSITORY_LOADER_CONFIG
    return loader.AvroMessageLoader(loader_config)


def test_avro_message_loader_init(avro_message_loader):
    """
    Tests AvroMessageLoader.init function

    Args:
        avro_message_loader: A test fixture which is a AvroMessageLoader
            with dependencies mocked away
    """
    assert avro_message_loader.topic == 'a'
    assert avro_message_loader.num_partitions == 10
    assert mock_avro_consumer.call_count == 1
    assert mock_avro_schema_registry.call_count == 1


@pytest.mark.parametrize(
    'key, num_partitions, expected_response',
    [(b'90', 100, 65), (b'15', 10, 8)]
)
def test_default_partitioner(key, num_partitions, expected_response):
    """
    Test the default partitioner with different parameters
    """
    response = loader.default_partitioner(key, num_partitions)
    assert expected_response == response


@patch('confluent_kafka_helpers.loader.TopicPartition', mock_topic_partition)
def test_avro_message_loader_load(avro_message_loader):
    message = conftest.PollReturnMock()
    conftest.mock_confluent_avro_consumer.poll.side_effect = [
        message, EndOfPartition
    ]
    avro_message_loader.key_serializer = lambda arg: arg

    messages = list(
        avro_message_loader.load(key=1, partitioner=mock_partitioner)
    )

    assert len(messages) == 1
    assert messages[0].value == b'foobar'


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

    @pytest.mark.parametrize('code', [
        (ConfluentKafkaError._ALL_BROKERS_DOWN),
        (ConfluentKafkaError._NO_OFFSET),
        (ConfluentKafkaError._TIMED_OUT)
    ])
    def test_raises_kafkaexception_on_other_errors(self, code):
        error = KafkaError(_code=code)
        with pytest.raises(KafkaException):
            loader.default_error_handler(error)
