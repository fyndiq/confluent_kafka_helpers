import zlib
from collections import defaultdict, deque
from copy import deepcopy
from typing import List, NamedTuple

DEFAULT_NUM_PARTITIONS = 8


class KafkaError:
    _PARTITION_EOF = -191


def singleton(cls):
    instances = {}

    def getinstance():
        if cls not in instances:
            instances[cls] = cls()
        return instances[cls]

    return getinstance


def default_partitioner(key, num_partitions):
    return zlib.crc32(key.encode('utf-8')) % num_partitions


class Partition(NamedTuple):
    topic: str
    partition: int
    messages: List


class Topic(list):
    def append(self, topic, partition, messages):
        partition = Partition(topic, partition, messages)
        super().append(partition)

    def __bool__(self):
        return any([p.messages for p in self])

    def __repr__(self):
        return f'Topic({list(self)})'


class MessageBatch(list):
    def __repr__(self):
        return f'MessageBatch({list(self)})'

    def __bool__(self):
        return any(self)


@singleton
class InMemoryLog(defaultdict):
    def __init__(self, *args, **kwargs):
        super().__init__(lambda: defaultdict(deque))

    def store_message(self, message):
        topic, partition = message.topic(), message.partition()
        offset = len(self[topic][partition])
        message._offset = offset
        self[topic][partition].append(message)

    def get_messages(self):
        # messages = []
        # for topic, partitions in deepcopy(self).items():
        #     topic_messages = []
        #     for partition, log in partitions.items():
        #         message = (topic, partition, log)
        #         topic_messages.append(message)
        #     messages.append(topic_messages)
        # return messages

        message_batch = MessageBatch()
        for topic_name, partitions in deepcopy(self).items():
            topic = Topic()
            for partition, messages in partitions.items():
                topic.append(topic_name, partition, messages)
            message_batch.append(topic)
        return message_batch


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

    def __init__(self, log: InMemoryLog = InMemoryLog):
        self._log = log()

    def store_offset(self, group_id, message):
        key = f'{group_id}{message.topic()}{message.partition()}'
        partition = default_partitioner(key, DEFAULT_NUM_PARTITIONS)
        self._log[self.topic][partition] = None

    def update_offsets(self):
        pass


class GroupCoordinator:
    def __init__(
        self, offset_manager: OffsetManager = OffsetManager,
        subscribed: SubscribedTopicPartitions = SubscribedTopicPartitions
    ) -> None:
        self._subscribed = subscribed
        self._subscribed_mapping = dict()
        self._offset_manager = offset_manager()

    def assign(self, group_id, topics_partitions):
        subscribed = self._subscribed().assign(topics_partitions)
        self._subscribed_mapping[group_id] = subscribed

    def subscribe(self, group_id, topics):
        subscribed = self._subscribed().subscribe(topics)
        self._subscribed_mapping[group_id] = subscribed

    def commit(self, group_id, message):
        self._offset_manager.store_offset(group_id, message)


class AdminAPI:
    def __init__(self, log: InMemoryLog = InMemoryLog) -> None:
        self._log = log()

    def get_topics(self):
        topics = {
            topic: [key for key, value in partitions.items()]
            for topic, partitions in self._log.items()
        }
        return topics

    def create_topics(self, topics, num_partitions):
        [
            self._log[topic][partition]
            for topic in topics
            for partition in range(num_partitions)
        ]
        # for topic in topics:
        #     for i in range(num_partitions):
        #         message_eof = Message(
        #             value=None, topic=topic, partition=i,
        #             error_code=KafkaError._PARTITION_EOF
        #         )
        #         partition_log = self._log[topic][i]
        #         offset = len(partition_log)
        #         if offset == 0:
        #             partition_log.append(message_eof)


@singleton
class Broker:
    def __init__(
        self, admin: AdminAPI = AdminAPI, log: InMemoryLog = InMemoryLog,
        group_coordinator: GroupCoordinator = GroupCoordinator
    ) -> None:
        self._log = log()
        self._admin = admin()
        self._group_coordinator = group_coordinator()

    @property
    def admin(self):
        return self._admin

    @property
    def num_partitions(self):
        return DEFAULT_NUM_PARTITIONS

    def subscribe(self, group_id, topics):
        self._group_coordinator.subscribe(group_id, topics)

    def assign(self, group_id, topics_partitions):
        self._group_coordinator.assign(group_id, topics_partitions)

    def commit(self, group_id, message):
        self._group_coordinator.commit(group_id, message)

    def send_message(self, message):
        self._admin.create_topics(
            [message.topic()], num_partitions=DEFAULT_NUM_PARTITIONS
        )
        self._log.store_message(message)

    def poll(self):
        return self._log.get_messages()


if __name__ == "__main__":
    broker = Broker()
    broker.send_message('1', '2', '3', 0)
    print(broker.admin.get_topics())
