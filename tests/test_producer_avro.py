from unittest.mock import MagicMock, patch

import pytest
from avro.schema import Schema
from confluent_kafka import avro

from confluent_kafka_helpers import producer
from tests import config

mock_avro_schema_registry = MagicMock()
mock_confluent_producer_impl_init = MagicMock()
mock_avro_producer_produce = MagicMock()


class Message(dict):
    _schema: Schema = None


message = Message({
    'name': 'some name'
})

value_schema_str = """
{
   "namespace": "my.test",
   "name": "value",
   "type": "record",
   "fields" : [
     {
       "name" : "name",
       "type" : "string"
     }
   ]
}
"""

message._schema = avro.loads(value_schema_str)


def teardown_function(function):
    mock_avro_producer_produce.reset_mock()


@pytest.fixture(scope='module')
@patch(
    'confluent_kafka_helpers.producer.Producer._init_producer_impl',
    mock_confluent_producer_impl_init
)
@patch('confluent_kafka_helpers.producer.Producer._close', MagicMock())
def avro_producer():
    producer_config = config.Config.KAFKA_AVRO_PRODUCER_CONFIG
    return producer.Producer(
        producer_config
    )


def test_producer_init(avro_producer):
    assert avro_producer.default_topic == 'c'
    assert mock_confluent_producer_impl_init.call_count == 1
    assert isinstance(avro_producer.value_serializer,
                      config.Config.KAFKA_AVRO_PRODUCER_CONFIG['value_serializer'])


@patch(
    'confluent_kafka_helpers.producer.Producer._produce',
    mock_avro_producer_produce
)
@patch(
    'confluent_kafka.avro.CachedSchemaRegistryClient.get_latest_schema',
    MagicMock(return_value=(1, message._schema, 1))
)
@patch(
    'confluent_kafka.avro.CachedSchemaRegistryClient.get_by_id',
    MagicMock(return_value=message._schema)
)
def test_avro_producer_produce_default_topic(avro_producer):
    key = 'a'
    value = message
    topic = 'c'
    avro_producer.produce(key=key, value=value)

    default_topic = avro_producer.default_topic
    assert topic == default_topic
    mock_avro_producer_produce.assert_called_once_with(
        topic=default_topic, key=key,
        value=avro_producer.value_serializer.serialize(value, topic)
    )


@patch(
    'confluent_kafka_helpers.producer.Producer._produce',
    mock_avro_producer_produce
)
@patch(
    'confluent_kafka.avro.CachedSchemaRegistryClient.get_latest_schema',
    MagicMock(return_value=(1, message._schema, 1))
)
@patch(
    'confluent_kafka.avro.CachedSchemaRegistryClient.get_by_id',
    MagicMock(return_value=message._schema)
)
def test_avro_producer_produce_specific_topic(avro_producer):
    key = 'a'
    value = message
    topic = 'z'
    avro_producer.produce(key=key, value=value, topic=topic)

    mock_avro_producer_produce.assert_called_once_with(
        topic=topic,
        key=key,
        value=avro_producer.value_serializer.serialize(value, topic)
    )

#
# def test_get_subject_names(avro_producer):
#     topic_name = 'test_topic'
#     key_subject_name, value_subject_name = (
#         avro_producer._get_subject_names(topic_name)
#     )
#     assert key_subject_name == (topic_name + '-key')
#     assert value_subject_name == (topic_name + '-value')
#
#
# def test_get_topic_schemas(avro_producer):
#     mock_avro_schema_registry.return_value.\
#         get_latest_schema.side_effect = ['1', '2']
#     topic_list = ['a']
#     topic_schemas = avro_producer._get_topic_schemas(topic_list)
#     topic, key_schema, value_schema = topic_schemas['a']
#     assert topic == 'a'
#     assert key_schema == '1'
#     assert value_schema == '2'
