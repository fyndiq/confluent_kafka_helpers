from collections import deque
from typing import NamedTuple

from confluent_kafka_helpers.mocks.kafka import Broker
from confluent_kafka_helpers.mocks.message import Message


class KafkaError:
    _PARTITION_EOF = -191


class TopicPartition(NamedTuple):
    topic: str
    partition: int = None


def roundrobin(*iterables):
    "roundrobin('ABC', 'D', 'EF') --> A D E B F C"
    iterators = deque(map(iter, iterables))
    while iterators:
        try:
            while True:
                yield next(iterators[0])
                iterators.rotate(-1)
        except StopIteration:
            iterators.popleft()


class MockConsumer:
    def __init__(self, config, broker: Broker = Broker):
        self._broker = broker()

    def poll(self, *args, **kwargs):
        messages = self._broker.prefetch_messages()
        for topic, partition, partition_log in roundrobin(*messages):
            while partition_log:
                return partition_log.popleft()
            # else:
            #     import ipdb; ipdb.set_trace()
            #     return Message(
            #         value=None, topic=topic, partition=partition,
            #         error_code=KafkaError._PARTITION_EOF
            #     )

    def subscribe(self, topics):
        topics_partition = [TopicPartition(topic=topic) for topic in topics]
        self._broker.consume_topics_partition(topics_partition)

    def close(self):
        pass

    def commit(self, *args, **kwargs):
        pass

    def assign(self, topics_partition):
        self._broker.consume_topics_partition(topics_partition)

    def unassign(self):
        pass


class MockAvroConsumer(MockConsumer):
    pass
