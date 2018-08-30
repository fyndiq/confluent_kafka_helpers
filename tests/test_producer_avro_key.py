from unittest.mock import MagicMock, patch

import pytest

from confluent_kafka_helpers import producer
from confluent_kafka_helpers.serialization import AvroStringKeySerializer
from tests import config

mock_avro_schema_registry = MagicMock()
mock_confluent_producer_impl_init = MagicMock()
mock_avro_producer_produce = MagicMock()


def teardown_function(function):
    mock_avro_producer_produce.reset_mock()


@pytest.fixture(scope='module')
@patch(
    'confluent_kafka_helpers.producer.Producer._init_producer_impl',
    mock_confluent_producer_impl_init
)
@patch('confluent_kafka_helpers.producer.Producer._close', MagicMock())
def avro_producer():
    producer_config = config.Config.KAFKA_AVRO_STRING_KEY_PRODUCER_CONFIG
    return producer.Producer(
        producer_config
    )


def test_producer_init(avro_producer):
    assert avro_producer.default_topic == 'c'
    assert mock_confluent_producer_impl_init.call_count == 1
    assert isinstance(
        avro_producer.key_serializer,
        config.Config.KAFKA_AVRO_STRING_KEY_PRODUCER_CONFIG['key.serializer']
    )


@patch(
    'confluent_kafka_helpers.producer.Producer._produce',
    mock_avro_producer_produce
)
@patch(
    'confluent_kafka.avro.CachedSchemaRegistryClient.get_latest_schema',
    MagicMock(return_value=(1, AvroStringKeySerializer.KEY_SCHEMA, 1))
)
@patch(
    'confluent_kafka.avro.CachedSchemaRegistryClient.get_by_id',
    MagicMock(return_value=AvroStringKeySerializer.KEY_SCHEMA)
)
def test_avro_producer_produce_default_topic(avro_producer):
    key = 'a'
    value = 'a'
    topic = 'c'
    avro_producer.produce(key=key, value=value)

    default_topic = avro_producer.default_topic
    assert topic == default_topic
    mock_avro_producer_produce.assert_called_once_with(
        topic=default_topic, key=b'\x00\x00\x00\x00\x01\x02a',
        value=value
    )


@patch(
    'confluent_kafka_helpers.producer.Producer._produce',
    mock_avro_producer_produce
)
@patch(
    'confluent_kafka.avro.CachedSchemaRegistryClient.get_latest_schema',
    MagicMock(return_value=(1, AvroStringKeySerializer.KEY_SCHEMA, 1))
)
@patch(
    'confluent_kafka.avro.CachedSchemaRegistryClient.get_by_id',
    MagicMock(return_value=AvroStringKeySerializer.KEY_SCHEMA)
)
def test_avro_producer_produce_specific_topic(avro_producer):
    key = 'a'
    value = 'a'
    topic = 'z'
    avro_producer.produce(key=key, value=value, topic=topic)

    mock_avro_producer_produce.assert_called_once_with(
        topic=topic,
        key=b'\x00\x00\x00\x00\x01\x02a',
        value=value
    )
