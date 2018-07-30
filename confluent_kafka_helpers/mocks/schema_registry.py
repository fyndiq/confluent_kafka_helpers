from typing import NamedTuple


class Schema(NamedTuple):
    id: int
    data: str
    version: int


class MockSchemaRegistry:
    def __init__(self, url):
        pass

    def get_latest_schema(self, subject):
        schema = Schema(id=1, data='foo', version=1)
        return schema.id, schema.data, schema.version

    def register(self, subject, schema):
        pass
