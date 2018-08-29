from unittest.mock import MagicMock, patch

import pytest

from tests import config

from confluent_kafka_helpers import producer

mock_avro_schema_registry = MagicMock()
mock_confluent_producer_impl_init = MagicMock()
mock_generic_producer_produce = MagicMock()


def teardown_function(function):
    mock_generic_producer_produce.reset_mock()


@pytest.fixture(scope='module')
@patch(
    'confluent_kafka_helpers.producer.Producer._init_producer_impl',
    mock_confluent_producer_impl_init
)
@patch('confluent_kafka_helpers.producer.Producer._close', MagicMock())
def generic_producer():
    producer_config = config.Config.KAFKA_REPOSITORY_PRODUCER_CONFIG
    return producer.Producer(
        producer_config  #, schema_registry=mock_avro_schema_registry
    )


def test_producer_init(generic_producer):
    producer_config = config.Config.KAFKA_REPOSITORY_PRODUCER_CONFIG
    assert generic_producer.default_topic == 'c'
    assert mock_confluent_producer_impl_init.call_count == 1


@patch(
    'confluent_kafka_helpers.producer.Producer._produce',
    mock_generic_producer_produce
)
def test_generic_producer_produce_default_topic(generic_producer):
    key = 'a'
    value = '1'
    topic = 'c'
    generic_producer.produce(key=key, value=value)

    default_topic = generic_producer.default_topic
    assert topic == default_topic
    mock_generic_producer_produce.assert_called_once_with(
        topic=default_topic, key=key,
        value=value
    )


@patch(
    'confluent_kafka_helpers.producer.Producer._produce',
    mock_generic_producer_produce
)
def test_generic_producer_produce_specific_topic(generic_producer):
    key = 'a'
    value = '1'
    topic = 'a'
    generic_producer.produce(key=key, value=value, topic=topic)

    mock_generic_producer_produce.assert_called_once_with(
        topic=topic, key=key, value=value
    )

#
# def test_get_subject_names(generic_producer):
#     topic_name = 'test_topic'
#     key_subject_name, value_subject_name = (
#         generic_producer._get_subject_names(topic_name)
#     )
#     assert key_subject_name == (topic_name + '-key')
#     assert value_subject_name == (topic_name + '-value')
#
#
# def test_get_topic_schemas(generic_producer):
#     mock_avro_schema_registry.return_value.\
#         get_latest_schema.side_effect = ['1', '2']
#     topic_list = ['a']
#     topic_schemas = generic_producer._get_topic_schemas(topic_list)
#     topic, key_schema, value_schema = topic_schemas['a']
#     assert topic == 'a'
#     assert key_schema == '1'
#     assert value_schema == '2'
