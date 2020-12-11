try:
    from confluent_kafka_helpers.tracing.backends.opentracer import OpenTracerBackend

    tracer = OpenTracerBackend()  # type: ignore
except ModuleNotFoundError:
    from confluent_kafka_helpers.tracing.backends.nulltracer import NullTracerBackend

    tracer = NullTracerBackend()  # type: ignore

__all__ = ['tracer']
