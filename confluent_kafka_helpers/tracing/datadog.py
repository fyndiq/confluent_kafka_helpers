import os

SPAN_TYPE = "span.type"
SPAN_TYPE_VALUE_CUSTOM = "custom"

OPERATION_NAME = "operation.name"
SERVICE_NAME = "service.name"
RESOURCE_NAME = "resource.name"


def get_datadog_service_name() -> str:
    return os.getenv("DD_SERVICE", "unknown_service")


def create_datadog_mappings(
    operation_name: str, service_name: str | None, resource_name: str | None
) -> dict:
    """
    dd-trace as of version 2.12.2 doesn't fully map OTEL -> Datadog semantics, more specifically:
      - span type (web, db, cache, custom)
      - operation name
      - service name
      - resource name

    In order to fully utilize Datadogs UI we must create these mappings manually.

    Unfortunately, there's no documentation about this, one would have to look into the file
    `ddtrace/internal/opentelemetry/span.py#_OTelDatadogMapping` to see the mappings.
    """
    mappings = {
        SPAN_TYPE: SPAN_TYPE_VALUE_CUSTOM,
        OPERATION_NAME: operation_name,
    }
    if service_name:
        mappings[SERVICE_NAME] = service_name
    if resource_name:
        mappings[RESOURCE_NAME] = resource_name

    return mappings
