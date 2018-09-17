from unittest.mock import MagicMock, patch

import pytest

from confluent_kafka_helpers import producer

from tests import config

mock_avro_schema_registry = MagicMock()
mock_confluent_avro_producer_init = MagicMock()
mock_avro_producer_produce = MagicMock()


def teardown_function(function):
    mock_avro_producer_produce.reset_mock()


@pytest.fixture(scope='module')
@patch(
    'confluent_kafka_helpers.producer.ConfluentAvroProducer.__init__',
    mock_confluent_avro_producer_init
)
@patch('confluent_kafka_helpers.producer.AvroProducer._close', MagicMock())
def avro_producer():
    producer_config = config.Config.KAFKA_REPOSITORY_PRODUCER_CONFIG
    return producer.AvroProducer(
        producer_config, schema_registry=mock_avro_schema_registry
    )


def test_avro_producer_init(avro_producer):
    producer_config = config.Config.KAFKA_REPOSITORY_PRODUCER_CONFIG
    assert avro_producer.default_topic == 'c'
    assert avro_producer.value_serializer == config.to_message_from_dto
    mock_avro_schema_registry.assert_called_once_with(
        producer_config['schema.registry.url']
    )
    assert mock_confluent_avro_producer_init.call_count == 1


@patch(
    'confluent_kafka_helpers.producer.ConfluentAvroProducer.produce',
    mock_avro_producer_produce
)
def test_avro_producer_produce_default_topic(avro_producer):
    key = 'a'
    value = '1'
    topic = 'c'
    avro_producer.produce(key=key, value=value)

    _, key_schema, value_schema = avro_producer.topic_schemas[topic]
    default_topic = avro_producer.default_topic
    mock_avro_producer_produce.assert_called_once_with(
        topic=default_topic, key=key,
        value=avro_producer.value_serializer(value), key_schema=key_schema,
        value_schema=value_schema
    )


@patch(
    'confluent_kafka_helpers.producer.ConfluentAvroProducer.produce',
    mock_avro_producer_produce
)
def test_avro_producer_produce_specific_topic(avro_producer):
    key = 'a'
    value = '1'
    topic = 'a'
    avro_producer.produce(key=key, value=value, topic=topic)

    topic, key_schema, value_schema = avro_producer.topic_schemas[topic]
    mock_avro_producer_produce.assert_called_once_with(
        topic=topic, key=key, value=avro_producer.value_serializer(value),
        key_schema=key_schema, value_schema=value_schema
    )


def test_get_subject_names(avro_producer):
    topic_name = 'test_topic'
    key_subject_name, value_subject_name = (
        avro_producer._get_subject_names(topic_name)
    )
    assert key_subject_name == (topic_name + '-key')
    assert value_subject_name == (topic_name + '-value')


def test_get_topic_schemas(avro_producer):
    mock_avro_schema_registry.return_value.\
        get_latest_schema.side_effect = ['1', '2']
    topic_list = ['a']
    topic_schemas = avro_producer._get_topic_schemas(topic_list)
    topic, key_schema, value_schema = topic_schemas['a']
    assert topic == 'a'
    assert key_schema == '1'
    assert value_schema == '2'
