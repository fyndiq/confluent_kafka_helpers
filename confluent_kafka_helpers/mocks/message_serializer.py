class MockMessageSerializer:
    def __init__(self, client):
        pass

    def encode_record_with_schema(self, topic, schema, key, is_key=True):
        return key.encode('utf-8')
