from confluent_kafka_helpers.serialization import AvroSerializer, \
    AvroStringKeySerializer


def to_message_from_dto(message):
    return message


class Config:

    KAFKA_CONSUMER_CONFIG = {
        'bootstrap.servers': 'localhost:29092',
        'group.id': 1,
        'schema.registry.url': 'localhost:8081',
        'topics': 'a'
    }

    KAFKA_REPOSITORY_LOADER_CONFIG = {
        'topic': 'a',
        'num_partitions': 10,
        'consumer': {
            'bootstrap.servers': 'localhost:29092',
            'group.id': 1,
            'schema.registry.url': 'localhost:8081',
        }
    }

    KAFKA_REPOSITORY_PRODUCER_CONFIG = {
        'bootstrap.servers': 'localhost:29092',
        'schema.registry.url': 'localhost:8081',
        'topics': ['c', 'a'],
    }

    KAFKA_AVRO_PRODUCER_CONFIG = {
        'bootstrap.servers': 'localhost:29092',
        'schema.registry.url': 'localhost:8081',
        'topics': ['c', 'a'],
        'value_serializer': AvroSerializer,
    }

    KAFKA_AVRO_STRING_KEY_PRODUCER_CONFIG = {
        'bootstrap.servers': 'localhost:29092',
        'schema.registry.url': 'localhost:8081',
        'topics': ['c', 'a'],
        'key_serializer': AvroStringKeySerializer
    }
