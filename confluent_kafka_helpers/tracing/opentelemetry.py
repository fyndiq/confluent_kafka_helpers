import contextlib
from typing import Callable, Iterator

from opentelemetry import trace
from opentelemetry.context.context import Context
from opentelemetry.semconv.schemas import Schemas
from opentelemetry.trace import Link, Span, SpanKind
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from . import attributes as attrs
from . import datadog

DEFAULT_EXT_SERVICE_NAME = "kafka"


class OpenTelemetryBackend:
    def __init__(self, name: str, tracer: Callable = trace.get_tracer) -> None:
        self._tracer = tracer(name, schema_url=Schemas.V1_27_0.value)

    @contextlib.contextmanager
    def start_span(
        self,
        name: str,
        service_name: str | None = DEFAULT_EXT_SERVICE_NAME,
        resource_name: str | None = None,
        kind: SpanKind = SpanKind.INTERNAL,
        system: str | None = attrs.MESSAGING_SYSTEM_VALUE_KAFKA,
        **kwargs,
    ) -> Iterator[Span]:
        span = self._tracer.start_span(name, kind=kind, **kwargs)

        if system:
            span.set_attribute(attrs.MESSAGING_SYSTEM, system)

        datadog_mappings = datadog.create_datadog_mappings(
            operation_name=name, service_name=service_name, resource_name=resource_name
        )
        for key, value in datadog_mappings.items():
            span.set_attribute(key, value)

        with trace.use_span(
            span, end_on_exit=True, record_exception=True, set_status_on_exception=True
        ):
            yield span

    def inject_headers(self, headers: dict) -> None:
        TraceContextTextMapPropagator().inject(headers)

    def extract_headers(self, headers: dict) -> Context:
        return TraceContextTextMapPropagator().extract(headers)

    def extract_links(self, context: Context) -> list[Link]:
        links: list = []
        if not context:
            return links
        for item in context.values():
            span_context = getattr(item, "get_span_context", None)
            if span_context:
                links.append(Link(context=span_context()))
        return links
