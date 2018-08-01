from collections import defaultdict, deque
from typing import NamedTuple

from confluent_kafka_helpers.mocks.kafka import Broker
from confluent_kafka_helpers.mocks.message import Message
from confluent_kafka_helpers.mocks.producer import MockProducer


class KafkaError:
    _PARTITION_EOF = -191


class TopicPartition(NamedTuple):
    topic: str
    partition: int = None
    offset: int = None


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


class SubscribedTopicPartitions(defaultdict):
    def __init__(self):
        super().__init__(list)

    def assign(self, topics_partitions):
        for topic_partition in topics_partitions:
            topic, partition = topic_partition.topic, topic_partition.partition
            self[topic] = partition

    def subscribe(self, topics):
        for topic in topics:
            self[topic]


class OffsetManager:
    topic = '__consumer_offsets'

    def __init__(
        self, group_id, subscribed: SubscribedTopicPartitions,
        producer: MockProducer = MockProducer
    ):
        self._producer = producer({})
        self._group_id = group_id
        self._subscribed = subscribed

    def commit(self, message):
        key = f'{self._group_id}{message.topic()}{message.partition()}'
        self._producer.produce(topic=self.topic, key=key, value=None)

    def update_offsets(self):
        import ipdb; ipdb.set_trace()
        pass


class MockConsumer:
    def __init__(
        self, config, broker: Broker = Broker,
        offset_manager: OffsetManager = OffsetManager,
        subscribed: SubscribedTopicPartitions = SubscribedTopicPartitions
    ):
        self._broker = broker()
        self._subscribed = subscribed()
        self._offset_manager = offset_manager(
            config['group.id'], subscribed=self._subscribed
        )
        self._message = None

    def poll(self, *args, **kwargs):
        self._offset_manager.update_offsets()
        # TODO: only return subscribed topic/partitions from last committed offset
        messages = self._broker.prefetch_messages()
        for topic, partition, partition_log in roundrobin(*messages):
            while partition_log:
                message = partition_log.popleft()
                self._message = message
                return message
            # else:
            #     import ipdb; ipdb.set_trace()
            #     return Message(
            #         value=None, topic=topic, partition=partition,
            #         error_code=KafkaError._PARTITION_EOF
            #     )

    def assign(self, topics_partitions):
        self._subscribed.assign(topics_partitions)

    def subscribe(self, topics):
        # topics_partition = [TopicPartition(topic=topic) for topic in topics]
        # self._broker.consume_topics_partition(topics_partition)
        self._subscribed.subscribe(topics)

    def commit(self, *args, **kwargs):
        self._offset_manager.commit(self._message)

    def close(self):
        pass

    def unassign(self):
        pass


class MockAvroConsumer(MockConsumer):
    pass
