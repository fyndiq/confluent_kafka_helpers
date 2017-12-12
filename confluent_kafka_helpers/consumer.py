
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.avro import AvroConsumer as ConfluentAvroConsumer
from confluent_kafka_helpers.message import Message


class AvroConsumer:

    DEFAULT_CONFIG = {
        'log.connection.close': False,
        'enable.auto.commit': False,
        'default.topic.config': {
            'auto.offset.reset': 'earliest'
        },
        'api.version.request': True
    }

    def __init__(self, config):
        self.config = {**self.DEFAULT_CONFIG, **config}
        self.poll_timeout = config.pop('poll_timeout', 0.1)
        self.topics = self._get_topics(self.config)

        self.consumer = ConfluentAvroConsumer(self.config)
        self.consumer.subscribe(self.topics)

    def __getattr__(self, name):
        return getattr(self.consumer, name)

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._message_generator())

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        # close down the consumer cleanly accordingly:
        #  - stops consuming
        #  - commit offsets (only on auto commit)
        #  - leave consumer group
        self.consumer.close()

        # the only reason a consumer exits is when an
        # exception is raised.

    def _message_generator(self):
        message = self.consumer.poll(timeout=self.poll_timeout)
        if message is None:
            yield None

        if message.error():
            if message.error().code() != KafkaError._PARTITION_EOF:
                raise KafkaException(message.error())

        yield message

    def _get_topics(self, config):
        topics = config.pop('topics', None)
        assert topics is not None, "You must subscribe to at least one topic"

        if not isinstance(topics, list):
            topics = [topics]

        return topics

    @property
    def is_auto_commit(self):
        return self.config.get('enable.auto.commit', True)
