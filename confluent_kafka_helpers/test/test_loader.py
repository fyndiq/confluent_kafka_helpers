from unittest.mock import MagicMock, patch

import pytest

from confluent_kafka_helpers import loader
from confluent_kafka_helpers.test import config

mock_avro_consumer = MagicMock()
mock_avro_schema_registry = MagicMock()


@pytest.fixture(scope='module')
@patch('confluent_kafka_helpers.loader.AvroConsumer', mock_avro_consumer)
@patch(
    'confluent_kafka_helpers.loader.AvroSchemaRegistry',
    mock_avro_schema_registry()
)
def avro_message_loader():
    loader_config = config.Config.KAFKA_REPOSITORY_LOADER_CONFIG
    return loader.AvroMessageLoader(loader_config)


def test_avro_message_loader_init(avro_message_loader):
    assert avro_message_loader.topic == 'a'
    assert avro_message_loader.key_subject_name == 'b'
    assert avro_message_loader.num_partitions == 10
    assert mock_avro_consumer.call_count == 1
    assert mock_avro_schema_registry.call_count == 1


@pytest.mark.parametrize('key, num_partitions, expected_response', [
    (b'90', 100, 65),
    (b'15', 10, 8)
])
def test_default_partitioner(key, num_partitions, expected_response):
    response = loader.default_partitioner(key, num_partitions)
    assert expected_response == response
