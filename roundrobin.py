from collections import defaultdict, deque
from itertools import chain
from typing import List, NamedTuple


class Partition(NamedTuple):
    topic: str
    partition: int
    messages: List


class Topic(list):
    def append(self, topic, partition, messages):
        partition = Partition(topic, partition, messages)
        super().append(partition)

    # def __bool__(self):
    #     return any([p.messages for p in self])

    def __repr__(self):
        return f'Topic({list(self)})'


class MessageBatch(list):
    def __repr__(self):
        return f'MessageBatch({list(self)})'

    # def __bool__(self):
    #     return any(self)


def check_eof(topic_partition_eof_map):
    partitions_eof = chain(
        *[
            list(partitions.values())
            for topic, partitions in topic_partition_eof_map.items()
        ]
    )
    return all(partitions_eof)


message_batch = MessageBatch(
    [
        Topic(
            [
                Partition(topic='test.test.commands', partition=0, messages=deque(['a', 'b'])),
                Partition(topic='test.test.commands', partition=1, messages=deque(['c'])),
                # Partition(topic='test.test.commands', partition=2, messages=deque(['d'])),
                # Partition(topic='test.test.commands', partition=3, messages=deque(['e', 'f']))
            ]
        ),
        Topic(
            [
                Partition(topic='test.test.events', partition=0, messages=deque(['g', 'h'])),
                # Partition(topic='test.test.events', partition=1, messages=deque(['i'])),
                # Partition(topic='test.test.events', partition=2, messages=deque(['j'])),
                # Partition(topic='test.test.events', partition=3, messages=deque(['k', 'l']))
            ]
        )
    ]
)

def poll():
    for topic in message_batch:
        for partition in topic:
            messages = partition.messages
            while messages:
                return partition.topic, partition.partition, messages.popleft()
            else:
                print(f"EOF {partition.topic}.{partition.partition}")
                del topic[0]
                return partition.topic, partition.partition, None
    else:
        print("No more messages in this batch")

eof = defaultdict(dict)
eof['test.test.commands'][0] = False
eof['test.test.commands'][1] = False
eof['test.test.events'][0] = False

while True:
    topic, partition, message = poll()
    if not message:
        eof[topic][partition] = True
        is_eof = check_eof(eof)
        if is_eof:
            raise StopIteration
    else:
        print(message)
