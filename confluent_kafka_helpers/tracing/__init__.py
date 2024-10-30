from . import datadog
from .opentelemetry import OpenTelemetryBackend

tracer = OpenTelemetryBackend("confluent_kafka_helpers")

__all__ = ['tracer', 'OpenTelemetryBackend', 'datadog']
