from confluent_kafka.avro import AvroProducer as ConfluentAvroProducer
from confluent_kafka_helpers import logger


class AvroProducer:

    def __init__(self, producer_config, key_schema, value_schema):
        self.producer = ConfluentAvroProducer(
            producer_config,
            default_key_schema=key_schema,
            default_value_schema=value_schema
        )

    def publish(self, topic, key, value):
        logger.info("Publishing message", topic=topic, key=key,
                    value=value)
        self.producer.produce(topic=topic, key=key, value=value)
        self.producer.flush()
