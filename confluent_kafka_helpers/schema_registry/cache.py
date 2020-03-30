class SchemaCache(dict):
    def update(self, schema, schema_id, subject):
        if not all([schema, schema_id, subject]):
            raise ValueError("Schema cache requires schema, schema_id and subject to be set")
        self[schema] = (schema, schema_id, subject)

    def get_schema_id(self, schema):
        try:
            _, schema_id, *_ = self[schema]
            return schema_id
        except KeyError:
            return None

    def get(self, schema):
        try:
            return self[schema]
        except KeyError:
            return None


schema_cache = SchemaCache()
