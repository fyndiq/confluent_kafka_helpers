# Agent guidelines

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

`confluent_kafka_helpers` is a library that wraps [confluent-kafka-python](https://github.com/confluentinc/confluent-kafka-python) with more Pythonic abstractions for producing and consuming Avro messages. It is published to PyPI as `confluent-kafka-helpers`. Python 3.10 is the target (`.python-version`, CircleCI).

## Commands

```bash
make setup        # create .venv and install requirements.txt
make unit-test    # pytest with coverage, writes junit xml to /tmp/test-results
make lint         # flake8 . + mypy confluent_kafka_helpers/
make test         # unit-test + lint (what CI effectively runs)
make build        # build wheel into dist/
make publish      # twine upload dist/*
make pip-update   # regenerate requirements.txt from requirements.in via pip-compile
```

Run a single test:

```bash
pytest tests/test_consumer.py::TestAvroConsumer::test_name
pytest tests/test_signals.py -k shutdown
```

Formatting/linting is enforced through flake8 plugins (flake8-black, flake8-isort, flake8-eradicate), so `make lint` fails on black/isort violations. Line length is 100 everywhere (black, isort, flake8). isort uses a custom section ordering with `FIRSTPARTY=confluent_kafka_helpers` and a separate `TEST=tests` section.

## Architecture

Three public entry points, all Avro-oriented and all wrapping a Confluent base class:

- **`AvroProducer`** (`producer.py`) — subclasses `ConfluentAvroProducer`. Requires a `topics` list at init; it pre-fetches key/value schemas from the schema registry for each topic (key schema optional for pub/sub-only topics). `produce()` resolves the schema per topic, applies an optional `value_serializer`, and raises `TopicNotRegistered` for unknown topics. Idempotence is on by default (`enable.idempotence`, `max.in.flight=1`). Registers an `atexit` flush.
- **`AvroConsumer`** (`consumer.py`) — *composes* (does not subclass) `ConfluentAvroConsumer`, delegating unknown attributes via `__getattr__`. It is an iterator/context-manager driven by an internal generator `_message_generator()`. `enable.auto.commit` is off by default; commit manually. Yields `Message` objects. Supports `non_blocking` (yield `None` when no message), `stop_on_eof`, and `poll_timeout` config keys (popped from config, not passed to Confluent).
- **`AvroMessageLoader`** (`loader.py`) — for *replaying* all messages for a single key from a topic (event-sourcing style "load aggregate"). Uses `AvroLazyConsumer` (a `ConfluentAvroConsumer` subclass that skips decoding in `poll()`) so it can filter by key *before* decoding. It deterministically computes the partition for a key (crc32 partitioner, matching Kafka's default) and assigns only that partition, then reads to EOF. `load()` returns a `MessageGenerator` that also detects/logs duplicate messages.

### Cross-cutting concerns

- **Graceful shutdown** (`__init__.py`) — **importing the package installs SIGTERM/SIGINT handlers as a side effect.** They set a process-wide `threading.Event` (`shutdown_requested`) instead of exiting; the consumer generator loop checks `is_shutdown_requested()` each iteration and finishes the in-flight message first. A *second* signal forces `sys.exit(0)`. Any pre-existing handler is captured at import time and chained (called after the flag is set). Public helpers: `is_shutdown_requested()`, `set/clear_shutdown_requested()`. The flag is global — tests must reset it (see the autouse fixture in `tests/test_signals.py`).
- **Tracing** (`tracing/`) — a single `tracer` instance (`OpenTelemetryBackend`) is created at import. Producer/consumer/schema-registry/commit operations are wrapped in spans via `tracer.start_span(...)`. W3C tracecontext is injected into Kafka headers on produce and extracted on consume. `datadog.py` manually maps OTEL span attributes to Datadog semantics (span type / operation / service / resource name) because ddtrace doesn't fully translate them — see the comment in `create_datadog_mappings`.
- **Header propagation** (`context.py`) — uses a `contextvars.ContextVar` to carry selected headers from a consumed message (configured via `headers.propagate` on the consumer) through to any messages produced while handling it. The consumer sets them per message; the producer merges them into outgoing headers.
- **Metrics** (`metrics/`) — Datadog statsd. **Disabled by default**: unless `DATADOG_ENABLE_METRICS=1` (and the `datadog` package is installed), `statsd` is a no-op null-object client. `base_metric` namespaces all metric names.
- **Callbacks** (`callbacks.py`) — `get_callback(custom, default)` wraps user callbacks so the default (which sends metrics) always runs, then the custom one. Defaults raise on errors (`KafkaError`, `KafkaDeliveryError`).
- **Retries** (`utils.py`) — `retry_exception` decorator retries on specific exception types with an optional `condition` predicate. Used for `_TRANSPORT` errors in `get_message` and transient `KafkaException`s on `commit`.

### Conventions

- Config dicts are passed through largely untouched to the Confluent clients; helper-specific keys (`topics`, `stop_on_eof`, `poll_timeout`, `non_blocking`, `headers.propagate`, `num_partitions`, etc.) are `pop`ped out before handing the rest to Confluent. When adding a new config option, follow this pop-then-merge-with-`DEFAULT_CONFIG` pattern.
- `Message` / `MessageMetadata` (`message.py`) use `__slots__` and normalize raw Confluent messages (decode headers, convert Kafka timestamps to `datetime`).
- Avro is mandatory throughout — there is no plain (non-Avro) consumer/producer.
