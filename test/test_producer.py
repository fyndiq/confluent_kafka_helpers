from unittest.mock import MagicMock, patch, ANY

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
    mock_avro_producer_produce.reset_mock()
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
    mock_avro_producer_produce.reset_mock()
    key = 'a'
    value = '1'
    topic = 't1'
    avro_producer.produce(key, value, topic=topic)

    mock_avro_producer_produce.assert_called_once_with(
        topic=topic,
        key=key,
        value=avro_producer.value_serializer(value),
        # TODO: Verify with the actual mocked schemas
        key_schema=ANY,
        value_schema=ANY,
    )


def test_get_default_subject_names(avro_producer):
    topic_name = 'test_topic'
    key_subject_name, value_subject_name = (
        avro_producer._get_default_subject_names(topic_name)
    )
    assert key_subject_name == (topic_name + '-key')
    assert value_subject_name == (topic_name + '-value')


def test_add_topic_data(avro_producer):
    topic = 'a'
    key_subject_name = 'b'
    value_subject_name = 'c'

    avro_producer.supported_topics = []
    avro_producer._add_topic_data(topic, key_subject_name, value_subject_name, MagicMock())
    assert len(avro_producer.supported_topics) == 1
    assert avro_producer.supported_topics[0]['topic'] == 'a'


def test_setup_extra_topics(avro_producer):
    topic_list = ['a', 'b']
    avro_producer.supported_topics = []
    avro_producer._setup_extra_topics(topic_list, MagicMock())

    assert len(avro_producer.supported_topics) == 2
    assert avro_producer.supported_topics[0]['topic'] == topic_list[0]
    assert avro_producer.supported_topics[1]['topic'] == topic_list[1]


def test_get_schemas(avro_producer):
    topic_list = ['a', 'b', 'c', 'd']
    topic_index = 0
    avro_producer.supported_topics = []

    avro_producer.supported_topics = []
    avro_producer._setup_extra_topics(topic_list, MagicMock())
    key_schema, value_schema = avro_producer._get_schemas(topic_list[topic_index])

    assert key_schema == avro_producer.supported_topics[topic_index]['key_schema']
    assert value_schema == avro_producer.supported_topics[topic_index]['value_schema']
