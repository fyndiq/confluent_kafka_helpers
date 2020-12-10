def to_message_from_dto(message):
    return message


class Config:

    KAFKA_CONSUMER_CONFIG = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 1,
        'schema.registry.url': '1.1.1.1',
        'topics': 'a',
    }

    KAFKA_REPOSITORY_LOADER_CONFIG = {
        'topic': 'a',
        'num_partitions': 10,
        'consumer': {
            'bootstrap.servers': 'localhost:9092',
            'group.id': 1,
            'schema.registry.url': '1.1.1.1',
        },
    }

    KAFKA_PRODUCER_CONFIG = {
        'bootstrap.servers': 'localhost:9092',
        'schema.registry.url': 'a',
        'topics': ['c', 'a'],
        'value_serializer': to_message_from_dto,
    }
