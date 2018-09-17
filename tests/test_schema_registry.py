from unittest.mock import MagicMock, patch

import pytest

from confluent_kafka_helpers import schema_registry

from tests import config


class CachedSchemaRegistryClientMock(MagicMock):
    get_latest_schema = MagicMock()
    get_latest_schema.return_value = ['a', 'b', 'c']
    register = MagicMock()


mock_client = CachedSchemaRegistryClientMock()
mock_serializer = MagicMock()


@pytest.fixture(scope='module')
def avro_schema_registry():
    url = config.Config.KAFKA_CONSUMER_CONFIG['schema.registry.url']
    return schema_registry.AvroSchemaRegistry(url, mock_client, mock_serializer)


def test_init(avro_schema_registry):
    mock_client.assert_called_once_with(
        url=config.Config.KAFKA_CONSUMER_CONFIG['schema.registry.url']
    )


def test_get_latest_schema(avro_schema_registry):
    subject = 'a'
    avro_schema_registry.get_latest_schema(subject)
    mock_client.get_latest_schema.assert_called_once_with(subject)


@patch('confluent_kafka_helpers.schema_registry.avro.load', MagicMock())
def test_register_schema(avro_schema_registry):
    avro_schema_registry.register_schema('a', 'b')
    assert mock_client.register.call_count == 1
