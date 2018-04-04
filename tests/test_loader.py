from unittest.mock import MagicMock, Mock, patch

import pytest

from tests import config, conftest

from confluent_kafka_helpers import loader

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
    # TODO: refactor
    # reset mock cause the messages has already been consumed at this point...
    message = conftest.PollReturnMock()
    conftest.mock_confluent_avro_consumer.poll.side_effect = [
        message, StopIteration
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
