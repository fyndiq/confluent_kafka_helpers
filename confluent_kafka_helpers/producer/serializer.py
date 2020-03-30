from confluent_kafka_helpers.schema_registry.client import schema_registry


def encode_record_with_schema(self, topic, schema, record, is_key=False):
    schema_id = schema_registry.get_schema_id_or_register(schema=schema, topic=topic, is_key=is_key)
    self.id_to_writers[schema_id] = self._get_encoder_func(schema)
    return self.encode_record_with_schema_id(schema_id, record, is_key=is_key)
