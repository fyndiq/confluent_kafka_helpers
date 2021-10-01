import contextlib
import io
import sys
import traceback

import opentracing

from confluent_kafka_helpers.tracing import tags

DEFAULT_TAGS = {
    tags.COMPONENT: 'confluent_kafka_helpers',
    tags.PEER_SERVICE: 'kafka',
}


class OpenTracerBackend:
    def __init__(self, tracer=opentracing.global_tracer):
        self._tracer = tracer()

    def __getattr__(self, name):
        return getattr(self._tracer, name)

    @contextlib.contextmanager
    def start_active_span(self, *args, **kwargs):
        # we can't use `start_active_span` or `start_span` built in context
        # managers since they default catches all exceptions and logs them
        # incorrectly in `opentracing.span.Span.__exit__` when using the
        # Datadog tracer.
        #
        # https://docs.datadoghq.com/tracing/visualization/trace/?tab=spantags
        scope = None
        try:
            scope = self._tracer.start_active_span(*args, tags=DEFAULT_TAGS, **kwargs)
            yield scope.span
        finally:
            if scope:
                self.log_exception(span=scope.span)
                scope.span.finish()
                scope.close()

    @contextlib.contextmanager
    def start_span(self, *args, **kwargs):
        span = None
        try:
            span = self._tracer.start_span(*args, tags=DEFAULT_TAGS, **kwargs)
            yield span
        finally:
            if span:
                self.log_exception(span=span)
                span.finish()

    @contextlib.contextmanager
    def inject_headers_and_start_span(self, operation_name, headers):
        try:
            with self.start_span(operation_name=operation_name) as span:
                self._tracer.inject(
                    span_context=span.context,
                    carrier=headers,
                    format=opentracing.Format.TEXT_MAP,
                )
                yield span
        finally:
            pass

    def extract_headers_and_start_span(self, operation_name, headers):
        try:
            parent_context = self._tracer.extract(
                carrier=headers, format=opentracing.Format.TEXT_MAP
            )
        except (
            opentracing.InvalidCarrierException,
            opentracing.SpanContextCorruptedException,
        ):
            span = self.start_active_span(operation_name=operation_name)
        else:
            span = self.start_active_span(
                operation_name=operation_name, references=[opentracing.follows_from(parent_context)]
            )
        return span

    def log_exception(self, span):
        exc_type, exc_value, exc_tb = sys.exc_info()
        if not exc_type:
            return

        buffer = io.StringIO()
        traceback.print_exception(exc_type, exc_value, exc_tb, file=buffer, limit=20)
        span.set_tag(opentracing.tags.ERROR, True)
        span.log_kv(
            {
                opentracing.logs.EVENT: opentracing.tags.ERROR,
                opentracing.logs.MESSAGE: str(exc_value),
                opentracing.logs.ERROR_OBJECT: exc_type.__name__,
                opentracing.logs.ERROR_KIND: exc_type.__name__,
                opentracing.logs.STACK: buffer.getvalue(),
            }
        )
