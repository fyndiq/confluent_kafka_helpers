import opentracing

from confluent_kafka_helpers.tracing import utils


class Tracer:
    def __init__(self):
        self._tracer = opentracing.global_tracer()

    def __getattr__(self, name: str):
        return getattr(self._tracer, name)

    def start_span(self, *args, **kwargs):
        return self._tracer.start_span(*args, **kwargs)

    def inject(self, *args, format=opentracing.Format.TEXT_MAP, **kwargs):
        self._tracer.inject(*args, format=format, **kwargs)

    def extract(self, headers: dict, format=opentracing.Format.TEXT_MAP):
        try:
            parent_context = self.tracer.extract(carrier=headers, format=format)
        except (
            opentracing.InvalidCarrierException,
            opentracing.SpanContextCorruptedException
        ):
            return None
        return parent_context


tracer = Tracer()
