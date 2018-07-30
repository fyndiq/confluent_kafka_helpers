import zlib

from confluent_kafka_helpers.mocks.kafka import Broker
from confluent_kafka_helpers.mocks.message import Message


def default_partitioner(key, num_partitions):
    return zlib.crc32(key.encode('utf-8')) % num_partitions


class MockAvroProducer:
    def __init__(self, config, broker: Broker = Broker):
        self._broker = broker()

    def flush(self):
        pass

    def produce(self, topic, key, value, **kwargs):
        partition = default_partitioner(key, self._broker.num_partitions)
        message = Message(
            value=value, topic=topic, key=key, partition=partition
        )
        self._broker.add_message(message)
