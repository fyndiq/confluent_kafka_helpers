from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.avro import AvroConsumer as ConfluentAvroConsumer
from confluent_kafka.avro.serializer import SerializerError

from confluent_kafka_helpers import logger


class AvroConsumer:

    def __init__(self, topic, config, timeout=0.1):
        if not isinstance(topic, list):
            self.topic = [topic]
        else:
            self.topic = topic
        self.config = config
        self.timeout = timeout

        self.consumer = ConfluentAvroConsumer(self.config)
        self.consumer.subscribe(self.topic)

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._message_generator())

    def _message_generator(self):
        message = self.consumer.poll(timeout=self.timeout)
        if not message:
            yield None

        if message.error():
            if message.error().code() != KafkaError._PARTITION_EOF:
                raise KafkaException(message.error())

        yield message

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type == SerializerError:
            logger.error("Deserialization error", error=exc_value)

        elif exc_type == KafkaException:
            logger.error("Kafka exception", error=exc_value)

        self.consumer.close()
