try:
    from confluent_kafka_helpers.tracing.backends.opentracer import OpenTracerBackend

    tracer = OpenTracerBackend()
except ModuleNotFoundError:
    from confluent_kafka_helpers.tracing.backends.nulltracer import NullTracerBackend

    tracer = NullTracerBackend()

__all__ = [tracer]
