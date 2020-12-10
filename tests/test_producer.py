from contextlib import ExitStack
from unittest.mock import MagicMock, call, patch

import pytest

from confluent_kafka_helpers import producer

from tests import config


@pytest.fixture
def confluent_avro_producer():
    producer = MagicMock()
    with ExitStack() as stack:
        stack.enter_context(
            patch('confluent_kafka_helpers.producer.ConfluentAvroProducer.__init__', MagicMock)
        )
        stack.enter_context(
            patch('confluent_kafka_helpers.producer.ConfluentAvroProducer.produce', producer)
        )
        yield producer


@pytest.fixture
@patch('confluent_kafka_helpers.producer.AvroProducer._close', MagicMock())
def avro_producer(confluent_avro_producer, avro_schema_registry):
    producer_config = config.Config.KAFKA_PRODUCER_CONFIG
    return producer.AvroProducer(producer_config, schema_registry=avro_schema_registry)


def test_avro_producer_init(avro_producer, avro_schema_registry):
    producer_config = config.Config.KAFKA_PRODUCER_CONFIG
    assert avro_producer.default_topic == 'c'
    assert avro_producer.value_serializer == config.to_message_from_dto
    avro_schema_registry.assert_called_once_with(producer_config['schema.registry.url'])


def test_avro_producer_produce_default_topic(confluent_avro_producer, avro_producer):
    key = 'a'
    value = '1'
    topic = 'c'
    avro_producer.produce(key=key, value=value)

    _, key_schema, value_schema = avro_producer.topic_schemas[topic]
    default_topic = avro_producer.default_topic
    confluent_avro_producer.assert_called_once_with(
        topic=default_topic,
        key=key,
        value=avro_producer.value_serializer(value),
        key_schema=key_schema,
        value_schema=value_schema,
        headers={},
    )


def test_avro_producer_produce_specific_topic(confluent_avro_producer, avro_producer):
    key = 'a'
    value = '1'
    topic = 'a'
    avro_producer.produce(key=key, value=value, topic=topic)

    topic, key_schema, value_schema = avro_producer.topic_schemas[topic]
    confluent_avro_producer.assert_called_once_with(
        topic=topic,
        key=key,
        value=avro_producer.value_serializer(value),
        key_schema=key_schema,
        value_schema=value_schema,
        headers={},
    )


@patch('confluent_kafka_helpers.producer.tracer')
def test_avro_producer_adds_tracing(tracer, avro_producer):
    avro_producer.produce(key='a', value='1', topic='a')
    expected_calls = [
        call.inject_headers_and_start_span(operation_name='kafka.producer.produce', headers={}),
        call.inject_headers_and_start_span().__enter__(),
        call.inject_headers_and_start_span().__enter__().set_tag('span.kind', 'producer'),
        call.inject_headers_and_start_span().__enter__().set_tag('message_bus.destination', 'a'),
        call.inject_headers_and_start_span().__enter__().set_tag('message_bus.key', 'a'),
        call.inject_headers_and_start_span().__exit__(None, None, None),
    ]
    tracer.assert_has_calls(expected_calls)


def test_get_subject_names(avro_producer):
    topic_name = 'test_topic'
    key_subject_name, value_subject_name = avro_producer._get_subject_names(topic_name)
    assert key_subject_name == (topic_name + '-key')
    assert value_subject_name == (topic_name + '-value')


def test_get_topic_schemas(avro_producer, avro_schema_registry):
    avro_schema_registry.return_value.get_latest_schema.side_effect = ['1', '2']
    topic_list = ['a']
    topic_schemas = avro_producer._get_topic_schemas(topic_list)
    topic, key_schema, value_schema = topic_schemas['a']
    assert topic == 'a'
    assert key_schema == '1'
    assert value_schema == '2'
