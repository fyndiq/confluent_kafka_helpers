def to_message_from_dto(message):
    return message


class Config:

    KAFKA_CONSUMER_CONFIG = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 1,
        'schema.registry.url': '1.1.1.1'
    }

    KAFKA_REPOSITORY_LOADER_CONFIG = {
        'topic': 'a',
        'key_subject_name': 'b',
        'num_partitions': 10,
        'consumer': KAFKA_CONSUMER_CONFIG.copy()
    }

    KAFKA_REPOSITORY_PRODUCER_CONFIG = {
            'bootstrap.servers': 'localhost:9092',
            'schema.registry.url': 'a',
            'key_subject_name': 'a',
            'value_subject_name': 'b',
            'default_topic': 'c',
            'value_serializer': to_message_from_dto,
            'extra_topics': ['t1', 't2']
    }
