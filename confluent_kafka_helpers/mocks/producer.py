import zlib

from confluent_kafka_helpers.mocks.kafka import Broker

NUM_PARTITIONS = 8


def default_partitioner(key, num_partitions=NUM_PARTITIONS):
    return zlib.crc32(key.encode('utf-8')) % num_partitions


class MockAvroProducer:
    def __init__(self, *args, broker: Broker = Broker):
        self._broker = broker()

    def flush(self):
        pass

    def produce(self, topic, key, value, **kwargs):
        partition = default_partitioner(key)
        self._broker.send_message(value, key, topic, partition)
