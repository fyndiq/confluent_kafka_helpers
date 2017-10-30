from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.avro import AvroConsumer as ConfluentAvroConsumer


class AvroConsumer:

    DEFAULT_CONFIG = {
        'log.connection.close': False,
        'enable.auto.commit': False
    }

    def __init__(self, topic, config, timeout=0.1):
        self.config = {**self.DEFAULT_CONFIG, **config}
        self.timeout = timeout

        if not isinstance(topic, list):
            self.topic = [topic]
        else:
            self.topic = topic

        self.consumer = ConfluentAvroConsumer(self.config)
        self.consumer.subscribe(self.topic)

    def __getattr__(self, name):
        return getattr(self.consumer, name)

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._message_generator())

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # close down the consumer cleanly accordingly:
        #  - stops consuming
        #  - commit offsets (only on auto commit)
        #  - leave consumer group
        self.consumer.close()

    def _message_generator(self):
        message = self.consumer.poll(timeout=self.timeout)
        if message is None:
            yield None

        if message.error():
            if message.error().code() != KafkaError._PARTITION_EOF:
                raise KafkaException(message.error())

        yield message

    @property
    def is_auto_commit(self):
        return self.config.get('enable.auto.commit', True)
