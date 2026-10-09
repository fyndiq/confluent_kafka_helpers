# Batch Consume Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add `AvroConsumer.batches()`, which yields `list[Message]` of up to `batch_max_size`, flushed early after `batch_max_wait` seconds.

**Architecture:** `batches()` reuses the existing `self._get_message()` poll path (retries, error handler, EOF) and buffers messages in `_collect_batch()`. One `kafka.consume` span per batch, linked to every message's trace context. Design: `docs/plans/2026-10-09-batch-consume-design.md`.

**Tech Stack:** Python, confluent-kafka (legacy `confluent_kafka.avro.AvroConsumer`), OpenTelemetry, pytest.

**Run tests with:** `.venv/bin/python -m pytest tests/test_consumer.py -v` (baseline: 34 passed). Lint: `make lint`.

**Ground rules:**
- All production changes go in `confluent_kafka_helpers/consumer.py` (plus one constant in `tracing/attributes.py`).
- Do not change `_message_generator`. Do not extract shared span helpers from it. `test_consume_messages_adds_tracing` asserts the exact `set_attribute` call order.
- Batch tests drive `poll()` with `ScriptedPoll` (defined in Task 2). It ends every test by requesting shutdown, so no test can loop forever.

---

### Task 1: Batch config keys

**Files:**
- Modify: `confluent_kafka_helpers/consumer.py:80-82`
- Test: `tests/test_consumer.py`

**Step 1: Write the failing tests**

Add to `class TestAvroConsumer`:

```python
    def test_batch_config_defaults(self, avro_consumer):
        consumer = avro_consumer()
        assert consumer.batch_max_size == 100
        assert consumer.batch_max_wait == 1.0

    def test_batch_config_is_popped_from_kafka_config(self, avro_consumer):
        consumer = avro_consumer(config_override={"batch_max_size": 5, "batch_max_wait": 2.5})

        assert consumer.batch_max_size == 5
        assert consumer.batch_max_wait == 2.5
        kafka_config = consumer._mock_consumer.call_args.args[0]
        assert "batch_max_size" not in kafka_config
        assert "batch_max_wait" not in kafka_config
```

**Step 2: Run to verify failure**

Run: `.venv/bin/python -m pytest tests/test_consumer.py -k batch_config -v`
Expected: FAIL with `AttributeError: ... 'batch_max_size'`. The `__getattr__` delegation sends the lookup to the mock, so the failure may be an assertion error comparing a `MagicMock` with `100`. Either one counts as a failure.

**Step 3: Implement**

In `AvroConsumer.__init__`, directly after `self.non_blocking = config.pop("non_blocking", False)`:

```python
        self.batch_max_size = config.pop("batch_max_size", 100)
        self.batch_max_wait = config.pop("batch_max_wait", 1.0)
```

**Step 4: Run to verify pass**

Run: `.venv/bin/python -m pytest tests/test_consumer.py -v`
Expected: all pass (36).

**Step 5: Commit**

```bash
git add confluent_kafka_helpers/consumer.py tests/test_consumer.py
git commit -m "feat(consumer): add batch_max_size and batch_max_wait config"
```

---

### Task 2: `batches()` core: size limit, shutdown flush, idle, generator close

**Files:**
- Modify: `confluent_kafka_helpers/consumer.py`
- Test: `tests/test_consumer.py`

**Step 1: Add test helpers and failing tests**

Add the following at the end of `tests/test_consumer.py`. The import `from unittest.mock import ANY, MagicMock, Mock, call, patch` already exists.

```python
class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


class ScriptedPoll:
    """poll() side effect: returns `items` in order, advancing `clock` by `tick` per call.
    When exhausted it requests shutdown and returns None, so iteration always ends."""

    def __init__(self, items, clock=None, tick=0.0):
        self.items = list(items)
        self.clock = clock
        self.tick = tick

    def __call__(self, timeout):
        if self.clock is not None:
            self.clock.now += self.tick
        if not self.items:
            confluent_kafka_helpers.set_shutdown_requested()
            return None
        return self.items.pop(0)


@pytest.fixture
def batch_consumer(avro_consumer):
    def _create(items, config_override=None, clock=None, tick=0.0):
        # large default max_wait so non-timing tests never flush on the real clock
        consumer = avro_consumer(config_override={"batch_max_wait": 60, **(config_override or {})})
        consumer.consumer.poll = MagicMock(side_effect=ScriptedPoll(items, clock, tick))
        return consumer

    return _create


class TestAvroConsumerBatches:
    def test_yields_full_batches_then_partial_on_shutdown(self, batch_consumer, confluent_message):
        message = confluent_message()
        consumer = batch_consumer([message] * 250, config_override={"batch_max_size": 100})

        batches = list(consumer.batches())

        assert [len(b) for b in batches] == [100, 100, 50]
        assert all(m.value == b"foobar" for b in batches for m in b)

    def test_blocking_idle_consumer_yields_nothing(self, batch_consumer):
        consumer = batch_consumer([None, None, None])

        assert list(consumer.batches()) == []

    def test_no_batches_if_shutdown_requested_before_iteration(
        self, batch_consumer, confluent_message
    ):
        consumer = batch_consumer([confluent_message()])
        confluent_kafka_helpers.set_shutdown_requested()

        assert list(consumer.batches()) == []

    def test_context_manager_closes_batch_generator(self, batch_consumer, confluent_message):
        consumer = batch_consumer([confluent_message()], config_override={"batch_max_size": 1})

        with consumer:
            batches = consumer.batches()
            next(batches)

        assert inspect.getgeneratorstate(batches) == inspect.GEN_CLOSED
```

Add `import inspect` at the top of the test file.

**Step 2: Run to verify failure**

Run: `.venv/bin/python -m pytest tests/test_consumer.py::TestAvroConsumerBatches -v`
Expected: FAIL. Because of `__getattr__`, `consumer.batches()` resolves to a mock attribute, and the assertions fail.

**Step 3: Implement**

In `consumer.py`, change `from typing import Callable` to `from typing import Callable, Iterator`. Add these methods to `AvroConsumer` after `_message_generator`:

```python
    def batches(self) -> Iterator[list[Message]]:
        """
        Yield lists of up to `batch_max_size` messages. A partial batch is yielded once
        `batch_max_wait` seconds have passed since its first message, and on shutdown.
        Commit after each batch. Don't mix with per-message iteration on the same consumer.
        """
        # stored as self._generator so __exit__ closes it, like the per-message generator
        self._generator = self._batch_generator()
        return self._generator

    def _batch_generator(self):
        while True:
            raw_messages, stop = self._collect_batch()
            if raw_messages or not stop:
                clear_propagated_headers()
                yield [Message(m) for m in raw_messages]
            if stop:
                break
        logger.info("Shutdown requested, exiting consumer loop")

    def _collect_batch(self) -> tuple[list, bool]:
        """Poll until the batch is full. Returns (raw messages, stop)."""
        batch: list = []
        while len(batch) < self.batch_max_size:
            if is_shutdown_requested():
                return batch, True
            message = self._get_message()
            if message is None:
                continue
            batch.append(message)
        return batch, False
```

**Step 4: Run to verify pass**

Run: `.venv/bin/python -m pytest tests/test_consumer.py -v`
Expected: all pass.

**Step 5: Commit**

```bash
git add confluent_kafka_helpers/consumer.py tests/test_consumer.py
git commit -m "feat(consumer): add batches() with size limit and shutdown flush"
```

---

### Task 3: `batch_max_wait` flush and `non_blocking`

**Files:**
- Modify: `confluent_kafka_helpers/consumer.py` (`_collect_batch`, imports)
- Test: `tests/test_consumer.py` (`TestAvroConsumerBatches`)

**Step 1: Write the failing tests**

```python
    def test_flushes_partial_batch_after_max_wait_since_first_message(
        self, batch_consumer, confluent_message
    ):
        message = confluent_message()
        clock = FakeClock()
        # each poll advances the clock 0.5s; first message at t=0.5 -> flush due at t=1.5
        consumer = batch_consumer(
            [message, None, None, message],
            config_override={"batch_max_wait": 1.0},
            clock=clock,
            tick=0.5,
        )

        with patch("confluent_kafka_helpers.consumer.monotonic", clock):
            batches = list(consumer.batches())

        assert [len(b) for b in batches] == [1, 1]

    def test_non_blocking_yields_empty_batch_after_max_wait(
        self, batch_consumer, confluent_message
    ):
        clock = FakeClock()
        consumer = batch_consumer(
            [None, None, None, confluent_message()],
            config_override={"batch_max_wait": 1.0, "non_blocking": True},
            clock=clock,
            tick=0.5,
        )

        with patch("confluent_kafka_helpers.consumer.monotonic", clock):
            batches = list(consumer.batches())

        assert [len(b) for b in batches] == [0, 1]
```

**Step 2: Run to verify failure**

Run: `.venv/bin/python -m pytest tests/test_consumer.py -k "max_wait" -v`
Expected: FAIL. `patch` raises `AttributeError: ... does not have the attribute 'monotonic'`.

**Step 3: Implement**

Add `from time import monotonic` to the stdlib imports in `consumer.py`, and replace `_collect_batch` with:

```python
    def _collect_batch(self) -> tuple[list, bool]:
        """Poll until the batch is full or due. Returns (raw messages, stop)."""
        batch: list = []
        # non-blocking: hand control back every batch_max_wait, even without messages.
        # blocking: the timer starts at the first message, so idle consumers never yield.
        deadline = monotonic() + self.batch_max_wait if self.non_blocking else None
        while len(batch) < self.batch_max_size:
            if is_shutdown_requested():
                return batch, True
            if deadline is not None and monotonic() >= deadline:
                break
            message = self._get_message()
            if message is None:
                continue
            batch.append(message)
            if deadline is None:
                deadline = monotonic() + self.batch_max_wait
        return batch, False
```

Note: in non-blocking mode the timer starts when collection starts, not at the first message. That is a small deviation from the design doc. It still meets "a message waits at most about `batch_max_wait`" and keeps one code path.

**Step 4: Run to verify pass**

Run: `.venv/bin/python -m pytest tests/test_consumer.py -v`
Expected: all pass.

**Step 5: Commit**

```bash
git add confluent_kafka_helpers/consumer.py tests/test_consumer.py
git commit -m "feat(consumer): flush batches after batch_max_wait"
```

---

### Task 4: EOF and error handling

**Files:**
- Modify: `confluent_kafka_helpers/consumer.py` (`_collect_batch`)
- Test: `tests/test_consumer.py` (`TestAvroConsumerBatches`)

**Step 1: Write the failing tests**

```python
    def test_stop_on_eof_yields_partial_batch_then_stops(self, batch_consumer, confluent_message):
        eof = confluent_message()
        eof.error.return_value = KafkaError(_code=ConfluentKafkaError._PARTITION_EOF)
        message = confluent_message()
        consumer = batch_consumer([message, message, eof], config_override={"stop_on_eof": True})

        batches = list(consumer.batches())

        assert [len(b) for b in batches] == [2]

    def test_error_propagates_and_buffered_messages_are_not_yielded(
        self, batch_consumer, confluent_message
    ):
        error = confluent_message()
        error.error.return_value = KafkaError(_code=ConfluentKafkaError._ALL_BROKERS_DOWN)
        consumer = batch_consumer([confluent_message(), error])

        yielded = []
        with pytest.raises(KafkaException):
            for batch in consumer.batches():
                yielded.append(batch)

        assert yielded == []
```

**Step 2: Run to verify failure**

Run: `.venv/bin/python -m pytest tests/test_consumer.py -k "eof or propagates" -v`
Expected: the EOF test FAILS with `EndOfPartition` raised. The error test may already pass, since there is no `except` that could swallow it. That's fine; it guards against regressions.

**Step 3: Implement**

In `_collect_batch`, replace `message = self._get_message()` with:

```python
            try:
                message = self._get_message()
            except EndOfPartition:  # only raised when stop_on_eof is set
                return batch, True
```

**Step 4: Run to verify pass**

Run: `.venv/bin/python -m pytest tests/test_consumer.py -v`
Expected: all pass.

**Step 5: Commit**

```bash
git add confluent_kafka_helpers/consumer.py tests/test_consumer.py
git commit -m "feat(consumer): stop batches on EOF when stop_on_eof is set"
```

---

### Task 5: Batch span, links and metrics

**Files:**
- Modify: `confluent_kafka_helpers/tracing/attributes.py:43`
- Modify: `confluent_kafka_helpers/consumer.py`
- Test: `tests/test_consumer.py` (`TestAvroConsumerBatches`)

**Step 1: Write the failing tests**

```python
    @patch("confluent_kafka_helpers.consumer.tracer")
    def test_one_span_per_batch_linked_to_each_message(
        self, tracer, batch_consumer, confluent_message
    ):
        tracer.extract_links.return_value = ["<link>"]
        message = confluent_message()
        consumer = batch_consumer([message] * 3)

        list(consumer.batches())

        tracer.start_span.assert_called_once_with(
            name="kafka.consume",
            kind=SpanKind.CONSUMER,
            resource_name="test",
            context=ANY,
            links=["<link>", "<link>", "<link>"],
        )
        assert tracer.start_span.call_args.kwargs["context"] == Context()  # new root trace
        span = tracer.start_span.return_value.__enter__.return_value
        span.set_attribute.assert_any_call("messaging.batch.message_count", 3)
        span.set_attribute.assert_any_call("messaging.destination.name", "test")
        span.set_attribute.assert_any_call("messaging.consumer.group.name", 1)
        span.set_attribute.assert_any_call("server.address", "localhost")

    @patch("confluent_kafka_helpers.consumer.tracer")
    def test_mixed_topic_batch_has_no_destination_name(
        self, tracer, batch_consumer, confluent_message
    ):
        other = confluent_message()
        other.topic.return_value = "other"
        consumer = batch_consumer([confluent_message(), other])

        list(consumer.batches())

        assert tracer.start_span.call_args.kwargs["resource_name"] == "other,test"
        span = tracer.start_span.return_value.__enter__.return_value
        assert call("messaging.destination.name", ANY) not in span.set_attribute.call_args_list

    @patch("confluent_kafka_helpers.consumer.set_propagated_headers")
    def test_does_not_set_propagated_headers(
        self, set_propagated_headers, batch_consumer, confluent_message
    ):
        consumer = batch_consumer(
            [confluent_message(headers=[("x-request-id", b"abc")])],
            config_override={"headers.propagate": ["x-request-id"]},
        )

        list(consumer.batches())

        set_propagated_headers.assert_not_called()

    @patch("confluent_kafka_helpers.consumer.statsd")
    def test_increments_message_count_by_batch_length(
        self, statsd, batch_consumer, confluent_message
    ):
        consumer = batch_consumer([confluent_message()] * 3)

        list(consumer.batches())

        statsd.increment.assert_called_once_with(
            "confluent_kafka_helpers.consumer.message.count.total", 3
        )
```

Add `from opentelemetry.context import Context` to the test imports.

**Step 2: Run to verify failure**

Run: `.venv/bin/python -m pytest tests/test_consumer.py -k "span or destination or propagated_headers or message_count" -v`
Expected: the span, destination and metric tests FAIL (`start_span` not called; `increment` not called). The propagated-headers test already passes, which is fine; it guards against regressions.

**Step 3: Implement**

In `tracing/attributes.py`, after `MESSAGING_CLIENT_ID = ...`:

```python
MESSAGING_BATCH_MESSAGE_COUNT = messaging_attributes.MESSAGING_BATCH_MESSAGE_COUNT
```

In `consumer.py`, add `import contextlib` (stdlib) and `from opentelemetry.context import Context`. In `_batch_generator`, replace the yield block:

```python
            if raw_messages or not stop:
                clear_propagated_headers()
                messages = [Message(m) for m in raw_messages]
                if not messages:  # non_blocking idle tick: nothing to trace
                    yield messages
                else:
                    with self._batch_span(messages):
                        yield messages
```

Add this method to `AvroConsumer` after `_collect_batch`. It intentionally duplicates a few lines from `_message_generator`; see ground rules.

```python
    @contextlib.contextmanager
    def _batch_span(self, messages: list[Message]):
        statsd.increment(f"{base_metric}.consumer.message.count.total", len(messages))

        topics = sorted({m._meta.topic for m in messages})
        links = [
            link
            for m in messages
            for link in tracer.extract_links(
                context=tracer.extract_headers(headers=m._meta.headers)
            )
        ]
        # no single message's trace is the rightful parent: start a new trace, link them all
        with tracer.start_span(
            name="kafka.consume",
            kind=SpanKind.CONSUMER,
            resource_name=",".join(topics),
            context=Context(),
            links=links,
        ) as span:
            span.set_attribute(attrs.MESSAGING_BATCH_MESSAGE_COUNT, len(messages))
            span.set_attribute(
                attrs.MESSAGING_OPERATION_NAME, attrs.MESSAGING_OPERATION_NAME_VALUE_CONSUME
            )
            span.set_attribute(
                attrs.MESSAGING_OPERATION_TYPE, attrs.MESSAGING_OPERATION_TYPE_VALUE_RECEIVE
            )
            span.set_attribute(attrs.MESSAGING_CLIENT_ID, self.client_id)
            span.set_attribute(attrs.MESSAGING_CONSUMER_GROUP_NAME, self.group_id)
            if len(topics) == 1:
                span.set_attribute(attrs.MESSAGING_DESTINATION_NAME, topics[0])

            server_address, *server_port = self.bootstrap_servers.split(":")
            span.set_attribute(attrs.SERVER_ADDRESS, server_address)
            if server_port:
                span.set_attribute(attrs.SERVER_PORT, server_port[0])

            yield span
```

Why `context=Context()` and not `None`: in OTEL, `None` means "use the current ambient context", which could make the batch a child of an unrelated span. An empty `Context()` forces a new root.

**Step 4: Run to verify pass**

Run: `.venv/bin/python -m pytest tests/test_consumer.py -v`
Expected: all pass.

**Step 5: Commit**

```bash
git add confluent_kafka_helpers/consumer.py confluent_kafka_helpers/tracing/attributes.py tests/test_consumer.py
git commit -m "feat(tracing): add batch consume span linked to message contexts"
```

---

### Task 6: README

**Files:**
- Modify: `README.md` (new section between "Graceful shutdown" and "OpenTelemetry (OTEL)")

**Step 1: Add the section**

````markdown
## Batch consumption

`AvroConsumer.batches()` yields lists of messages, e.g. for bulk writes:

```python
consumer = AvroConsumer({
    ...,
    "batch_max_size": 100,  # default 100
    "batch_max_wait": 1.0,  # seconds, default 1.0
})

for batch in consumer.batches():
    bulk_write([message.value for message in batch])
    consumer.commit(asynchronous=False)
```

- A batch is yielded when it has `batch_max_size` messages, or `batch_max_wait` seconds after
  its first message, whichever comes first. An idle consumer yields nothing
  (with `non_blocking`, it yields `[]` every `batch_max_wait`).
- On shutdown (or EOF with `stop_on_eof`) the partial batch is yielded, then iteration stops.
- `commit()` without arguments commits everything polled so far, i.e. the whole batch.
- Don't mix `batches()` and per-message iteration on the same consumer.

### Tracing

Each batch gets one `kafka.consume` span. It starts a new trace and links to every message's
trace context. Propagated headers are not set; read them from `message._meta.headers`.

OpenTelemetry keeps at most 128 links per span by default and silently drops the rest. If you raise
`batch_max_size` above 128, also raise `OTEL_SPAN_LINK_COUNT_LIMIT`.
````

**Step 2: Commit**

```bash
git add README.md
git commit -m "docs: document batch consumption"
```

---

### Task 7: Final verification

**Step 1:** Run: `.venv/bin/python -m pytest tests/ -q`
Expected: all pass. Baseline was 34 in `test_consumer.py`; this plan adds 14, so expect 48 there.

**Step 2:** Run: `make lint`
Expected: exit 0. Fix only issues in files touched by this plan.

**Step 3:** Smoke-run the real consumer code path against mocks one more time:
`.venv/bin/python -m pytest tests/test_consumer.py::TestAvroConsumerBatches -v`
Expected: 12 passed.
