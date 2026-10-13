# Confluent Kafka helpers

[![build](https://circleci.com/gh/fyndiq/confluent_kafka_helpers/tree/master.svg?style=shield)](https://circleci.com/gh/fyndiq/confluent_kafka_helpers/tree/master)
[![version](https://img.shields.io/pypi/v/confluent-kafka-helpers.svg)](https://pypi.org/project/confluent-kafka-helpers/)
[![downloads](https://img.shields.io/pypi/dm/confluent-kafka-helpers.svg)](https://pypi.org/project/confluent-kafka-helpers/)
[![license](https://img.shields.io/pypi/l/confluent-kafka-helpers.svg)](https://pypi.org/project/confluent-kafka-helpers/)

Library built on top of [Confluent Kafka
Python](https://github.com/confluentinc/confluent-kafka-python) adding
abstractions for consuming and producing messages in a more Pythonic way.

## Graceful shutdown

Importing `confluent_kafka_helpers` installs handlers for `SIGTERM` and `SIGINT`
that **do not exit the process immediately**. Instead the in-flight message handler always runs to completion before the loop exits.

### Chaining with other signal handlers

If another library (ddtrace, OpenTelemetry SDK, gunicorn, uvicorn, ...) has
already installed a SIGTERM/SIGINT handler before `confluent_kafka_helpers`
is imported, the existing handler is captured and **still called** after the
flag is set.

### Polling the flag from your own code

Long-running handlers can check the flag and bail out early between subtasks:

```python
from confluent_kafka_helpers import is_shutdown_requested

for message in consumer:
    do_step_one(message)
    if is_shutdown_requested():
        break
    do_step_two(message)
```

### Operational notes

- `SIGKILL` cannot be intercepted. Kubernetes will still send it after
  `terminationGracePeriodSeconds` if the process hasn't exited. The default value for this in k8s is 30 seconds. If handling a single message can take longer than that the application need to configure a longer `terminationGracePeriodSeconds` in the k8s manifest
- The flag is process-wide. Tests that exercise it must clear it (see
  `tests/test_signals.py` for the autouse reset fixture pattern).

## OpenTelemetry (OTEL)

### Test generation of spans

Make sure you have `opentelemetry-sdk` installed.

Add this code to your applications entry point as early as possible, e.g:

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

provider = TracerProvider()
processor = BatchSpanProcessor(ConsoleSpanExporter())
provider.add_span_processor(processor)
trace.set_tracer_provider(provider)

...
consumer = init_app()
consumer.consume()
```

All spans will now be printed to the std out instead of getting exported.

**NOTE!** This will most probably conflict if you use another SDK, e.g: `ddtrace` with OTEL enabled
(`DD_TRACE_OTEL_ENABLED=true`), since it will set up it's own read-only tracer provider. This is mainly for debugging
the generated spans from an OTEL point of view. Once verified, you should also test with your preferred SDK/exporter.

### Kafka operation spans and commit metrics

Successful message handling emits `kafka.consume` and `kafka.produce`; message
construction and value serialization run inside those spans. This library emits
no separate message construction span; application handler spans such as those
from `eventsourcing_helpers` are unaffected. Serialization
failures therefore mark `kafka.produce` as an error, including without a parent
span. `kafka.poll` and `kafka.commit` spans are emitted only for failures.
Polling and commit attempt error spans include the elapsed operation time.
`kafka.flush` tracing is unchanged.

Every explicit commit attempt, including each retry, emits these StatsD metrics:

- `confluent_kafka_helpers.consumer.commit.count.total`
- `confluent_kafka_helpers.consumer.commit.count.error` on failure
- `confluent_kafka_helpers.consumer.commit.duration`, a distribution in seconds
  measured with a monotonic clock

Tags are `consumer_group`, `mode` (`sync` or `async`), `phase:attempt`,
`outcome` (`success`, `error`, or `no_offset`), and `error_code` when a Kafka error is available.
`_NO_OFFSET` results are tagged `outcome:no_offset` and are not counted or traced as errors.
Synchronous commits also inspect returned partition errors without
changing their return or retry behavior. Each failed attempt or callback emits
one span using the overall error or the first failed partition as its representative
exception, prioritizing other errors over `_NO_OFFSET`. Asynchronous attempt duration measures
submission only, not broker completion latency.

The `on_commit` callback also emits total/error counts with `phase:completion`
and `mode:async_or_auto`, and emits an error span for a broker or partition
failure. It preserves the configured user callback. Completion callbacks cannot
reliably be matched to explicit submissions or automatic commits, so they emit
no duration metric and their error spans start when the callback runs. Filter
metrics by phase to avoid counting async submission and completion together.

Metrics require `DATADOG_ENABLE_METRICS=1` and the installed `datadog` client.
Tracing must be enabled with a recording provider to emit error spans. Emitting
an error span does not guarantee that Datadog ingests or retains it. To keep all
commit errors, configure a Datadog single-span sampling rule for `kafka.commit`
with `sample_rate: 1` and no `max_per_second` cap, then a 100% retention filter
matching `env:prod operation_name:kafka.commit status:error`. Put that filter
before other matching filters that could retain fewer spans, and verify the
SDK/exporter and Datadog configuration in the application environment.

For upgraded workloads, merge this rule with any existing
[`DD_SPAN_SAMPLING_RULES`](https://ddtrace.readthedocs.io/en/stable/configuration.html#DD_SPAN_SAMPLING_RULES):

```sh
DD_SPAN_SAMPLING_RULES='[{"name":"kafka.commit","sample_rate":1}]'
```

Apply this rule only to workloads using the error-only commit instrumentation;
older versions also emit successful commit spans and would retain them all.
Do not overwrite existing sampling rules. Configure the 100%
[Datadog retention filter](https://docs.datadoghq.com/tracing/trace_pipeline/trace_retention/)
separately using the query and ordering described above.
