import pytest

from unittest.mock import patch, MagicMock

from confluent_kafka_helpers import producer
from confluent_kafka_helpers.test import config

mock_avro_schema_registry = MagicMock()
mock_confluent_avro_producer_init = MagicMock()

@pytest.fixture(scope='module')
@patch('confluent_kafka_helpers.producer.AvroSchemaRegistry', mock_avro_schema_registry)
@patch('confluent_kafka_helpers.producer.ConfluentAvroProducer.__init__', mock_confluent_avro_producer_init)
def avro_producer():
    producer_config = config.Config.KAFKA_REPOSITORY_PRODUCER_CONFIG
    return producer.AvroProducer(producer_config)



def test_avro_producer_init(avro_producer):
    assert avro_producer.default_topic == 'c'
    assert avro_producer.value_serializer == config.to_message_from_dto
    assert mock_avro_schema_registry.call_count == 1
    assert mock_confluent_avro_producer_init.call_count == 1


mock_avro_producer_produce = MagicMock()
mock_avro_producer_flush = MagicMock()
@patch('confluent_kafka_helpers.producer.ConfluentAvroProducer.produce', mock_avro_producer_produce)
@patch('confluent_kafka_helpers.producer.ConfluentAvroProducer.flush', mock_avro_producer_flush)
def test_avro_producer_produce(avro_producer):
    key = 'a'
    value = '1'
    avro_producer.produce(key, value)
    mock_avro_producer_produce.assert_called_once_with(
        topic=avro_producer.default_topic,
        key=key,
        value=avro_producer.value_serializer(value)
    )
    assert mock_avro_producer_flush.call_count == 1

