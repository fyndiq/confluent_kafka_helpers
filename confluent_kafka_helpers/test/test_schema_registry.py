import pytest

from unittest.mock import patch, MagicMock

from confluent_kafka_helpers import schema_registry
from confluent_kafka_helpers.test import config


class CachedSchemaRegistryClientMock(MagicMock):
    get_latest_schema = MagicMock()
    get_latest_schema.return_value = ['a', 'b', 'c']
    register = MagicMock()


mock_cached_schema_registry_client = CachedSchemaRegistryClientMock()


@pytest.fixture(scope='module')
@patch(
    'confluent_kafka_helpers.schema_registry.CachedSchemaRegistryClient',
    mock_cached_schema_registry_client
)
def avro_schema_registry():
    url = config.Config.KAFKA_CONSUMER_CONFIG['schema.registry.url']
    return schema_registry.AvroSchemaRegistry(url)


def test_init(avro_schema_registry):
    mock_cached_schema_registry_client.assert_called_once_with(
       url=config.Config.KAFKA_CONSUMER_CONFIG['schema.registry.url']
    )


def test_get_latest_schema(avro_schema_registry):
    subject = 'a'
    avro_schema_registry.get_latest_schema(subject)
    mock_cached_schema_registry_client.get_latest_schema.assert_called_once_with(
        subject
    )


@patch('confluent_kafka_helpers.schema_registry.avro.load', MagicMock())
def test_register_schema(avro_schema_registry):
    avro_schema_registry.register_schema('a', 'b')
    assert mock_cached_schema_registry_client.register.call_count == 1

