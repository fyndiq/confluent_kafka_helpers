import enum

from confluent_kafka import avro


class SubjectNameStrategies(enum.Enum):
    TOPICNAME = 'TopicName'
    RECORDNAME = 'RecordName'
    TOPICRECORDNAME = 'TopicRecordName'


def is_key(schema_file: str) -> bool:
    return schema_file.replace('.avsc', '').endswith('-key')


class SubjectNameResolver:
    @staticmethod
    def factory(strategy: str):
        if strategy == SubjectNameStrategies.TOPICNAME.value:
            return TopicNameStrategyResolver
        elif strategy == SubjectNameStrategies.RECORDNAME.value:
            return RecordNameStrategyResolver
        elif strategy == SubjectNameStrategies.TOPICRECORDNAME.value:
            return TopicRecordNameStrategyResolver
        else:
            raise RuntimeError(f"Invalid subject name strategy: {strategy}")


class TopicNameStrategyResolver(SubjectNameResolver):
    def get_subject(self, schema_file: str):
        pass

    def get_schema(self, topic: str, record_name: str):
        pass


class RecordNameStrategyResolver(SubjectNameResolver):
    def get_subject(self, schema_file: str):
        pass

    def get_schema(self, topic: str, record_name: str):
        pass


class TopicRecordNameStrategyResolver(SubjectNameResolver):
    def get_subject(self, schema_file: str) -> str:
        schema = avro.load(schema_file)
        fullname = schema.fullname
        return f'{fullname}-key' if is_key(schema_file) else f'{fullname}-value'

    def get_schema(self, topic, record_name):
        pass
