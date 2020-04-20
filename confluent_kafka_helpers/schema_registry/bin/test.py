import sys

import structlog
from confluent_kafka import avro

from confluent_kafka_helpers.schema_registry.bin import utils
from confluent_kafka_helpers.schema_registry.client import schema_registry

logger = structlog.get_logger(__name__)


def test_compatibility(
    schema_file: str, subject: str = None, topic: str = None, is_key: bool = False
) -> None:
    compatible = schema_registry.test_compatibility(
        schema=avro.load(schema_file), subject=subject, topic=topic, is_key=is_key
    )
    if not compatible:
        logger.error("Schema not compatible with latest version")
        sys.exit(1)
    logger.info("Schema is compatible with latest version")


def run(automatic: bool, folder: str, schema_file: str, subject: str, topic: str) -> None:
    if automatic:
        for schema_file, is_key in utils.get_schema_files(folder=folder):
            test_compatibility(schema_file=schema_file, is_key=is_key)
    else:
        test_compatibility(schema_file=schema_file, subject=subject, topic=topic)
