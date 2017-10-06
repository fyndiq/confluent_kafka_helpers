from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.avro import AvroConsumer as ConfluentAvroConsumer


class AvroConsumer:

    # for available configuration options see:
    # https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    DEFAULT_CONFIG = {
        'session.timeout.ms': 6000,
        'log.connection.close': False
    }

    def __init__(self, topic, config, timeout=0.1):
        if not isinstance(topic, list):
            self.topic = [topic]
        else:
            self.topic = topic

        config.update(self.DEFAULT_CONFIG)
        self.timeout = timeout

        self.consumer = ConfluentAvroConsumer(config)
        self.consumer.subscribe(self.topic)

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._message_generator())

    def _message_generator(self):
        message = self.consumer.poll(timeout=self.timeout)
        if message is None:
            yield None

        if message.error():
            if message.error().code() != KafkaError._PARTITION_EOF:
                raise KafkaException(message.error())

        yield message

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.consumer.close()
