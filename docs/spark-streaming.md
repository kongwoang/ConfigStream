# Spark Structured Streaming

## Version matrix

| Component | Selected version |
| --- | --- |
| Python | 3.12 (project constraint) |
| Apache Spark / PySpark | 4.0.2, pinned optional dependency |
| JDK | 21; existing local Homebrew OpenJDK 21.0.12.1; CI uses Temurin 21 |
| Scala binary version | 2.13, required by the Spark 4.0 distribution |
| Spark Kafka connector | `org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2` |
| Kafka broker | Existing `apache/kafka:4.1.2`, unchanged |

Spark 4.0.2 is a maintenance release on the 4.0 line, which supports Python 3.9+
and Java 17/21. This project targets Java 21 consistently for local use and CI;
no JDK is bundled or installed by the project. The current machine already has
Java 21. Python 3.12 fits that matrix without changing existing schemas/generator.
The Spark distribution and Kafka connector share the **exact** version and Scala
binary suffix. The separate confluent-kafka Python client cannot supply this JAR.
Transitive connector dependencies are resolved by Spark, not manually copied.

Check `java -version` (expected major 21), and point `JAVA_HOME` to that JDK when
multiple versions exist. Runtime checks reject missing/wrong Java and PySpark.
Use `make spark-setup` to install locked Kafka + Spark extras. `make setup` remains
the lightweight Phase 3 install and removes the unselected Spark extra when syncing.
Download/build caches stay under `.cache/`; no JDK, downloaded JAR, or checkpoint
belongs in Git. Initial connector resolution needs access to Maven repositories.

## Implemented flow and boundaries

The authoritative entry point is `.venv/bin/python -m spark.streaming.main`,
wrapped by `make spark-stream`. It starts local Spark, sets UTC, loads the matching
Kafka connector, and uses `readStream.format("kafka")` directly. It does not create
a Python consumer, call `json.loads` in a UDF, or validate rows with Pydantic.

Only `config.snapshots` is subscribed. `config.events` remains an independent audit
stream, and `config.assets` is not joined yet. There is no cross-topic ordering
assumption. Each Kafka row flows through metadata extraction, SQL envelope parsing,
basic validation, then a safe console projection. Normalization, configuration
comparison, alerts, deduplication, business watermarks, and storage are absent.

## Configuration

Environment parsing is centralized in `spark/common/config.py`; it reuses the
existing Kafka bootstrap default without depending on the producer/client.
Export variables explicitly; Python does not load `.env` automatically.

| Variable | Default | Meaning |
| --- | --- | --- |
| `SPARK_MASTER` | `local[*]` | Local host threads; use `local[2]` to limit CPU |
| `SPARK_APP_NAME` | `ConfigStreamStreaming` | Spark application name |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Host-accessible broker address |
| `SPARK_CHECKPOINT_DIR` | `.cache/spark-checkpoints/config-snapshots` | One query's persistent progress |
| `SPARK_RUNTIME_DIR` | `.cache/spark` | Ivy JARs, scratch space, JVM temp files, warehouse location |
| `SPARK_STARTING_OFFSETS` | `latest` | `latest`, `earliest`, or explicit snapshot partition offsets |
| `SPARK_TRIGGER_SECONDS` | `2` | Processing-time micro-batch trigger |
| `SPARK_MAX_OFFSETS_PER_TRIGGER` | `10000` | Rate limit in source offsets, not a throughput guarantee |
| `SPARK_CONSOLE_ROWS` | `20` | Maximum displayed rows per batch |

`JAVA_HOME`/`PATH` select the external JDK. Spark SQL session timezone is explicitly
UTC and timestamp type is `TIMESTAMP_LTZ`. Local tests use `local[2]`, two shuffle
partitions, no UI, and always stop the session. The application also disables the
UI and uses two shuffle partitions. The Kafka source disables topic auto creation,
retains `failOnDataLoss=true`, and limits metadata API timeouts/retries so a broken
broker is a visible failure rather than a silent skip.

## SQL schema and parsing

`spark/common/schemas.py` mirrors current `schemas.ConfigSnapshot` field names:

- Strings: `schema_version`, `snapshot_id`, `asset_id`, `config_format`, `hash`,
  `content`, `content_uri`.
- `version`: Spark LongType (signed 64-bit).
- `event_time`, `ingest_time`: strings in the JSON StructType, then converted to
  Spark TimestampType with `try_to_timestamp` and an explicit ISO-8601 pattern.

The envelope StructType contains `record_type`, nested `payload`, and an internal
`_corrupt_record` marker. `from_json` uses PERMISSIVE parsing: syntax/type errors
become diagnostics, including truncated JSON that leaves partially populated
fields. Single-quoted JSON and non-numeric-number extensions are disabled.
The raw/corrupt JSON string is not carried into console output. Raw timestamp
strings remain inside the parsed frame for validation; they are also excluded
from the clean valid projection and console.

Timezone-aware ISO-8601 timestamps with `T`, seconds, optional fractional seconds,
and `Z` or `±HH:MM` offsets are accepted and represented as UTC instants. Naive,
missing, and impossible timestamps are invalid. Spark keeps microsecond precision.
The parser remains safe with Spark 4's ANSI mode enabled.

The original Kafka columns are retained as `kafka_key` (UTF-8 string), `kafka_topic`,
`kafka_partition`, `kafka_offset`, and `kafka_timestamp`. Kafka timestamp is broker
record metadata, not the generator's simulated event/ingestion time. Offsets are
per-partition log positions, not globally ordered snapshot versions.

## Basic validation

All checks are Spark column expressions. `validation_error` is the **first**
failure in the following order; valid records have null error and status `valid`:

| Check | Diagnostic |
| --- | --- |
| Parseable envelope, no corrupt marker (including null/tombstone values) | `malformed_json` |
| `record_type == config_snapshot` | `wrong_record_type` |
| Object payload present | `missing_payload` |
| Explicit `schema_version == 1.0` | `unsupported_schema_version` |
| Snapshot ID has non-whitespace text | `missing_snapshot_id` |
| Asset ID has non-whitespace text | `missing_asset_id` |
| Parsed integer version > 0 | `invalid_version` |
| Nonblank config format | `invalid_config_format` |
| Valid timezone-aware event / ingestion timestamps | `invalid_event_time` / `invalid_ingest_time` |
| Hash shaped as 64 lowercase hex characters | `invalid_hash` |
| Exactly one non-null content location; URI cannot be blank | `invalid_content_location` |
| Kafka key equals asset ID exactly, including case | `kafka_key_mismatch` |

Empty inline content is valid. No hash recomputation, URI fetching/complete URI
validation, unknown-field rejection, per-asset version ordering, or cross-record
consistency is implemented here. Spark's permissive string-field coercion and
64-bit integer representation also differ from full Pydantic transport validation.
Thus `valid` means these Phase 4 checks passed, not complete schema equivalence or
safe-to-display content. Type errors flagged by the parser take precedence over
field-specific errors. Schemas and generator semantics have not been changed.

`valid_snapshots(classified)` exposes the ten snapshot fields plus the five Kafka
metadata columns, retaining unmodified content internally. `console_rows` projects
only status/error, valid asset/version/snapshot/event-time/hash, and Kafka metadata.
For invalid records it suppresses payload-derived display columns entirely. It
never includes configuration, URI, or raw JSON; no raw-content debug mode exists.
Identifiers/keys themselves may still be sensitive and logs need appropriate access.

## One console query, checkpoint, and progress

The console sink is `writeStream.format("console").outputMode("append")`, with
`truncate=false` and a row display cap. **One classified query** provides both valid
rows and clearly labeled invalid diagnostics. This avoids two independent Kafka
readers/checkpoints drifting apart. The stream does not filter out invalid rows,
but the console only shows a sample when a batch exceeds the configured row cap.
It is not a durable audit store, invalid-record archive, or DLQ producer.

The checkpoint is a dedicated local directory, never shared by concurrent queries.
Spark records offsets, commits, and query metadata there. `latest` starts after
offsets captured during initial query initialization; records already in Kafka are
not replayed. Wait for the initial progress message before running a live demo.
`earliest` reads retained history, not deleted data. Explicit JSON partition offsets
are supported for bounded integration tests; every current partition must be listed:
`{"config.snapshots":{"0":10,"1":0,"2":0,"3":0,"4":8,"5":0}}`.

**After a checkpoint exists, saved progress wins over startingOffsets.** Changing
`latest` to `earliest` with the same checkpoint does not request a replay. Use a new
directory for replay, after changing query shape, or after resetting/recreating
Kafka topics. No automatic destructive checkpoint-reset command is provided.
Progress checkpoints do not make console output durable or exactly once. A failure
around output/commit may repeat output; topic retention can make resumption fail.

Default trigger is two seconds. `--available-now` processes data available when
the query starts, potentially across multiple rate-limited batches, then exits.
Later arrivals require another run; this is useful for reproducible bounded tests.

The app logs startup, subscription, checkpoint, query start/stop, and sampled native
progress: `batchId`, `numInputRows`, `inputRowsPerSecond`, `processedRowsPerSecond`.
These count source records (valid plus invalid), not just valid snapshots. The
monitor polls `query.lastProgress`; very fast intermediate batches may be omitted
from logs. Python callers can inspect `query.recentProgress` for recent batches.
No benchmark or production-latency claim follows from these local smoke metrics.

Connector resolution, schema initialization, unavailable Kafka, and query failures
exit nonzero with a visible error. Unexpected continuous-query termination also
fails. Ctrl-C requests clean query/session shutdown. Spark logging is reduced to
WARN after startup, not muted; JVM/package startup warnings can still appear.

## Run and test

Run commands from the project root, with Docker already available:

```sh
java -version
make setup
make check
make spark-setup
make spark-test
make kafka-up
make kafka-topics
```

Terminal A (choose a previously unused checkpoint path):

```sh
SPARK_MASTER='local[2]' SPARK_STARTING_OFFSETS=latest \
SPARK_CHECKPOINT_DIR=.cache/spark-checkpoints/live-demo \
make spark-stream
```

After the initial progress log, terminal B:

```sh
make kafka-demo
.venv/bin/python -m scripts.kafka_invalid_demo
```

Expect 8 valid snapshots, then 4 invalid diagnostics: malformed JSON, wrong type,
missing asset ID, and key mismatch. The generator also sends assets/events, but
Spark does not subscribe to them. The test helper deliberately bypasses the normal
validated producer; never use it against production topics. Its fixtures contain
no real secrets. The query must remain alive after these records.

```sh
make integration-test
make spark-integration-test
```

The existing Kafka suite remains separate. The Spark integration test records
starting offsets with the Kafka Admin API, publishes 8 changes and 4 malformed
fixtures, and reads Kafka directly through Spark. It uses `availableNow`, bounded
timeouts, five offsets per trigger, and a tiny test-only `foreachBatch` collector
for assertions. Production uses the console sink, not that collector. Assertions
check 8 valid / 4 invalid, keys, timestamps, provenance, then restart from the same
checkpoint with `earliest`: no old records return, and a subsequent new snapshot
does return. Tests assume no concurrent topic writers and leave small fixture
records in the local broker. They neither delete topics nor reset user checkpoints.
Another integration test points the real CLI at a bound but non-listening local
port, verifying that an unavailable broker produces a nonzero exit and visible
timeout. A service-free Spark test verifies that omitting the connector fails
instead of falling back to Python ingestion.

Stop terminal A with Ctrl-C, then `make kafka-down`. To replay retained history
without leaving a process running:

```sh
SPARK_STARTING_OFFSETS=earliest \
SPARK_CHECKPOINT_DIR=.cache/spark-checkpoints/replay-demo \
make spark-stream SPARK_ARGS=--available-now
```

`make check` stays fast and does not require Java, Spark, or Kafka. `make spark-test`
uses tiny static Spark DataFrames without downloading the Kafka connector. CI runs
it in a separate Java 21 job; full Kafka/Spark integration remains an explicit local
command. The Spark extra is large on first install (PySpark bundles the Spark JARs).

## Recorded local acceptance

On macOS arm64, Python 3.12.14, OpenJDK 21.0.12.1, Spark/PySpark 4.0.2, and the
unchanged Kafka 4.1.2 broker, the following checks passed:

- `make check`: Ruff lint/format and 316 fast unit tests.
- `make spark-test`: 36 local Spark tests, no broker/connector downloads required.
- `make integration-test`: 3 existing Kafka tests.
- `make spark-integration-test`: 2 real Kafka/Spark and failure-boundary tests.
- Live console with a fresh `latest` checkpoint: exactly 8 valid snapshots from
  `make kafka-demo`, plus 4 separately injected invalid diagnostics. Keys,
  versions, parsed event times, hash shape, partitions, and offsets were checked.
  Neither configuration bodies nor the malformed fixture marker appeared in logs.
- The query remained live after bad records. Ctrl-C stopped query/session without
  a Python traceback. A signal handler only sets a shutdown flag, avoiding Py4J
  calls from inside the signal handler while another gateway call is in progress.
- Native console `--available-now` replayed retained test history, then emitted
  zero new records on a second run with the same checkpoint despite `earliest`.
- The Phase 2 JSONL demo checksum remains
  `ae9fb7b3b347c069822fd1a208d85573b0ddad959971b63aadd439af75db0af2`.

The live command used `SPARK_MASTER='local[2]'`, `SPARK_STARTING_OFFSETS=latest`,
and `SPARK_CHECKPOINT_DIR=.cache/spark-checkpoints/phase4-live-2`. Bounded console
runs used `earliest`, `.cache/spark-checkpoints/phase4-bounded-1`, and
`make spark-stream SPARK_ARGS=--available-now`, twice. Those paths now contain
progress; use new paths to reproduce the first-run behavior. These are correctness
smoke checks, not scalability, latency, or fault-tolerance experiments.

## Limitations and next phases

Local mode is a single host, not evidence of distributed-cluster throughput or HA.
Kafka remains the Phase 3 loopback-only plaintext single-broker development setup.
No watermark, deduplication, asset join, stateful diff, normalization, rule engine,
alert/DLQ producer, MinIO, Elasticsearch, API, or cloud deployment is included.
Phase 5 should add tested generic text normalization/secret masking and diff logic,
not change the ingestion contract. Stateful streaming policies belong to Phase 6.

## References

- [Spark 4.0.2 supported runtimes](https://spark.apache.org/docs/4.0.2/)
- [Structured Streaming Kafka source](https://spark.apache.org/docs/4.0.2/streaming/structured-streaming-kafka-integration.html)
- [Streaming APIs, progress, and checkpoints](https://spark.apache.org/docs/4.0.2/streaming/apis-on-dataframes-and-datasets.html)
