class Partition:
    def __init__(self, partition):
        self._partition = partition

    def __getattr__(self, attr):
        return self._partition[attr]

    def __repr__(self):
        return f'<Partition: {self.partition}>'


class Partitions(list):
    def __init__(self, partitions):
        for key, value in partitions.items():
            self.append(Partition(value))

    def __repr__(self):
        partitions = ', '.join([repr(b) for b in self])
        return f'[{partitions}]'


class Topic:
    def __init__(self, topic):
        self._topic = topic
        self._partitions = Partitions(self._topic['partitions'])

    def __getattr__(self, attr):
        return self._topic[attr]

    def __repr__(self):
        return f'<Topic: {self.topic}>'

    @property
    def partitions(self):
        return self._partitions


class Topics(list):
    def __init__(self, topics):
        for key, value in topics.items():
            self.append(Topic(value))

    def __repr__(self):
        topics = ', '.join([repr(b) for b in self])
        return f'[{topics}]'
