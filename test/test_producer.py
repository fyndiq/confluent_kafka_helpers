from unittest.mock import MagicMock, patch

import pytest

from confluent_kafka_helpers import producer
from test import config

mock_avro_schema_registry = MagicMock()
mock_confluent_avro_producer_init = MagicMock()
mock_avro_producer_produce = MagicMock()


@pytest.fixture(scope='module')
@patch(
    'confluent_kafka_helpers.producer.AvroSchemaRegistry',
    mock_avro_schema_registry
)
@patch(
    'confluent_kafka_helpers.producer.ConfluentAvroProducer.__init__',
    mock_confluent_avro_producer_init
)
def avro_producer():
    producer_config = config.Config.KAFKA_REPOSITORY_PRODUCER_CONFIG
    return producer.AvroProducer(producer_config)


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
    avro_producer.produce(key, value)
    mock_avro_producer_produce.assert_called_once_with(
        topic=avro_producer.default_topic,
        key=key,
        value=avro_producer.value_serializer(value)
    )


@patch(
    'confluent_kafka_helpers.producer.ConfluentAvroProducer.produce',
    mock_avro_producer_produce
)
def test_avro_producer_produce_specific_topic(avro_producer):
    key = 'a'
    value = '1'
    topic = 't1'
    import ipdb; ipdb.set_trace()
    avro_producer.produce(key, value, topic=topic)
    mock_avro_producer_produce.assert_called_once_with(
        topic=topic,
        key=key,
        value=avro_producer.value_serializer(value)
    )


def test_get_default_subject_names(avro_producer):
    topic_name = 'test_topic'
    import ipdb; ipdb.set_trace()
    key_subject_name, value_subject_name = (
        avro_producer._get_default_subject_names(topic_name)
    )
    assert key_subject_name == (topic_name + '-key')
    assert value_subject_name == (topic_name + '-value')

