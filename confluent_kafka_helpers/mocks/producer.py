from confluent_kafka_helpers.mocks.kafka import Broker, default_partitioner
from confluent_kafka_helpers.mocks.message import Message


class MockProducer:
    def __init__(self, config, broker: Broker = Broker):
        self._broker = broker()

    def flush(self):
        pass

    def produce(self, topic, key, value, **kwargs):
        partition = default_partitioner(key, self._broker.num_partitions)
        message = Message(
            value=value, topic=topic, key=key, partition=partition
        )
        return self._broker.send_message(message)


class MockAvroProducer(MockProducer):
    pass
