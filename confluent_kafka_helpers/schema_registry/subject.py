class SubjectNameStrategies:
    TOPICNAME = 'TopicName'
    RECORDNAME = 'RecordName'
    TOPICRECORDNAME = 'TopicRecordName'


class SubjectNameResolver:
    def __init__(self, strategy: str):
        if strategy == SubjectNameStrategies.TOPICNAME:
            self._resolver = TopicNameStrategyResolver()
        elif strategy == SubjectNameStrategies.RECORDNAME:
            self._resolver = RecordNameStrategyResolver()
        elif strategy == SubjectNameStrategies.TOPICRECORDNAME:
            self._resolver = TopicRecordNameStrategyResolver()
        else:
            raise RuntimeError(f"Invalid subject name strategy: {strategy}")

    def get_subject(self, schema: str, topic: str = None, is_key: bool = False) -> str:
        return self._resolver.get_subject(schema=schema, topic=topic, is_key=is_key)


class TopicNameStrategyResolver:
    def get_subject(self, topic: str, is_key: bool, **kwargs) -> str:
        if not topic:
            raise ValueError("Topic must be specified when using TopicName strategy")
        return "-".join([topic, 'key' if is_key else 'value'])


class RecordNameStrategyResolver:
    def get_subject(self, schema: str, **kwargs) -> str:
        if not schema:
            raise ValueError(
                "Schema must be specified when using RecordName strategy"
            )
        return schema.name


class TopicRecordNameStrategyResolver:
    def get_subject(self, schema: str, is_key: bool, **kwargs) -> str:
        if not schema:
            if is_key:
                return None
            raise ValueError(
                "Schema must be specified when using TopicRecordName strategy"
            )
        return "-".join([schema.fullname, 'key' if is_key else 'value'])
