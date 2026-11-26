# Batch processing through Kafka and eventsourcing helpers

## Contract

Add opt-in batch consumption and publishing through both libraries, then demonstrate service adoption in article-handler-service. Keep single-message APIs unchanged. One consumer uses either single-message or batch iteration, never both concurrently. Batch processing is at least once. Finish database work, confirm Kafka output delivery, then commit explicit input offsets. Failures stop consumption for replay. Kafka orders records within each partition. Applications own replay-safe state changes and output effects.

Native Consumer.consume(num_messages, timeout) preserves message headers and exists in the locked confluent-kafka 2.8.0. Legacy AvroConsumer only deserializes poll(), so batch records require explicit Avro key/value decoding. Native produce_batch does not support headers; bulk publishing must enqueue ordinary asynchronous produce calls and wait once, allowing librdkafka to batch network traffic.

Sources: [consumer implementation](https://github.com/confluentinc/confluent-kafka-python/blob/v2.8.0/src/confluent_kafka/src/Consumer.c), [Avro implementation](https://github.com/confluentinc/confluent-kafka-python/blob/master/src/confluent_kafka/avro/__init__.py), [librdkafka API](https://github.com/confluentinc/librdkafka/blob/master/src/rdkafka.h), [consumer tests](https://github.com/confluentinc/confluent-kafka-python/blob/master/tests/test_Consumer.py), [ducktape tests](https://github.com/confluentinc/confluent-kafka-python/blob/master/tests/ducktape/test_consumer.py).

## Kafka helpers

- Add AvroBatchConsumer(config, batch_size=100, timeout=0.1), a context-managed iterator of lists of Message, and commit_batch(messages). Share existing setup and serialization where practical.
- Fetch once per batch with native consume; decode keys and values with existing reader schemas. Preserve tombstones, metadata, and returned order. Batch size is a maximum from 1 to 1,000,000; timeout must be finite and nonnegative. Deliver partial batches and skip empty callback batches.
- Handle EOF across assigned partitions and deliver remaining data before stopping. Fail the complete fetch before application delivery on record or deserialization errors; never discard a failed batch by fetching another one.
- Disable auto commit and auto offset storage for batch consumers, rejecting conflicting explicit settings. Commit highest successfully processed offset plus one per topic-partition synchronously. Check partition-level errors and propagate assignment loss rather than swallowing it.
- Snapshot configured propagated headers per message: existing default keys plus headers.propagate. Preserve the existing decoded text-header mapping contract.
- Add optional source=message to publishing. Merge that source's propagation snapshot with explicit output headers, with explicit business headers winning. Never choose an arbitrary batch member as source. Output without a source has no implicit input provenance in batch processing.
- Provide a public per-message context manager for sequential fallback handlers. Activate that message's propagation and trace context, and restore previous context with tokens in finally, including exceptions and nesting.
- Add produce_many(records), where each record contains normal production arguments plus optional source. Preserve schema lookup, serialization, tracing, callbacks, and metrics. On BufferError retry only the rejected record, servicing callbacks while waiting.
- Use a batch processing span linked to input contexts and source-associated output spans. Clean up context on exceptions and generator closure.

## Eventsourcing helpers

- Add MessageBus.consume_batch(handler, batch_size=100, timeout=0.1, delivery_timeout=30.0), get_batch_consumer(batch_size=100, timeout=0.1), and produce_many(records). Extend both mock backends and the backend interface.
- Extend Consumer.consume_batch to call Handler.handle_batch; the default implementation raises NotImplementedError. Existing handlers and DTO conversion retain their contracts and Meta headers.
- Pass the complete ordered list to one application callback. Do not group by command type automatically.
- Track every output produced through the bus during a batch, including fallback ordinary produce calls. Preserve configured callbacks and metrics. Suppress per-record flush, then wait once after successful handler return. Delivery failure, timeout, outstanding output, or callback failure prevents acknowledgement. Bound queue backpressure as well as delivery waiting.
- Outside consumption, produce_many follows the existing flush option once per collection.
- Filter watchdog-seen messages before dispatch; mark newly handled messages only after handler success and delivery. Commit seen records safely without invoking an empty callback. Stop on failure and rely on restart/replay, without partial acknowledgements or implicit handler retries.

## Article service adoption

Expose batch size and timeout in service configuration. Add an explicit KafkaMessageBus.produce_many wrapper respecting ENABLE_MESSAGEBUS_PRODUCE; __getattr__ would bypass this guard. Wire the actual init_app consumer construction to the batch-capable handler when opted in.

Implement a PauseArticle bulk path first. Decode DTOs retaining their source messages, validate using the existing schema, bulk fetch and lock affected articles in stable order, preserve missing/deleted/blocked handling, apply accepted changes in bulk, and commit once. Existing model helpers commit per row and must not be used for this bulk write. Return one result per applicable command in input order, preserving repeated IDs and source associations. Replayed pauses still emit events.

The application constructs produce_many entries with key=article.id, value=events.create_article_updated_event(...), topic=inventory_article_events, source=input_message, headers={"origin": article.origin}. Consumption owns the final output delivery barrier and input commit.

Support incremental migration: process contiguous supported command runs in bulk, and dispatch other commands sequentially through their existing handlers inside the per-message context wrapper. Preserve input order; never globally regroup commands by type. Fallbacks retain per-record database costs and must tolerate batch replay.

Keep batch adoption opt-in. Verify replay safety for each handler before broad rollout. CreateArticle currently returns when an article exists, which can suppress an event after a crash between database commit and publication. The application owns how it handles that gap.

## Verification and delivery

- Test full, partial, empty, size-one and invalid batches; partitions; Avro decoding; tombstones; EOF; failures; shutdown and assignment loss.
- Test differing propagation and trace headers across records, explicit overrides, one-to-many output, absent headers, fallback context restoration, DTOs and both mocks.
- Test queue pressure after accepted output, early callbacks, delivery failures and timeout, one delivery barrier, and no commit/watchdog advancement on failure.
- Test explicit partition offsets, offset gaps, seen records, commit errors and restart replay.
- Run both libraries' unit suites, flake8 and mypy, plus service unit and PostgreSQL integration tests. Verify the full pipeline against local minikube Kafka using the service's testdata/CreateArticle.sh conventions, consuming output to check headers. Use isolated test records and avoid disturbing existing consumer groups.
- Benchmark sizes 1, 10, 100 and 500 under comparable payloads/headers; measure throughput, database calls, commits, latency, memory and lag. Keep processing plus delivery within max.poll.interval.ms.
- Release Kafka helpers before raising the eventsourcing dependency floor; then adopt in the service. Temporary editable installs are allowed for cross-repository verification and must not leak local paths into committed dependencies.
- Implement sequentially across Kafka helpers, eventsourcing helpers, and article-handler-service. Create one draft PR per repository using the pr skill, documenting material remaining rollout limitations.
