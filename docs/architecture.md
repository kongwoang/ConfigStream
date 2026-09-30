# Architecture

## Scope and status

ConfigStream focuses on distributed configuration-change storage and processing.
Phases 0–4 supply a Python project, tested versioned schemas, a stateful generator,
JSONL output, Kafka ingestion with an asset registry, and direct Spark Structured
Streaming parsing/validation to console. The generator can still run without
external services. Normalization, diffs, rules, data-lake/serving sinks, APIs,
Kubernetes, and monitoring systems remain future work.

## Implemented synthetic source

`generator/engine.py` maintains a list of per-asset states and selects an asset
using a local seeded RNG. Type-specific templates and mutation domains are data;
the mutation and versioning logic is shared across all asset types. Each mutation
constructs the existing ChangeEvent and ConfigSnapshot models, then advances only
the selected state. CLI pacing is shared across JSONL and Kafka sinks, outside
the engine. `messaging/serialization.py` is the common envelope encoder;
`generator/serialization.py` retains its Phase 2 compatibility interface.

`messaging/kafka_producer.py` accepts domain models and owns routing, asset keys,
callback polling, bounded backpressure, delivery failures, and final flushing.
confluent-kafka is an optional, lazily imported dependency; generator state and
schemas have no dependency on its internals. See [generator lifecycle](generator.md).

## Implemented Kafka ingestion

Kafka is the ingestion/message backbone, not an application database. At startup,
Kafka mode sends N Asset envelopes to `config.assets`, keyed by `asset_id`, and
waits for this registry batch to be acknowledged before generating changes. It
does not resend Asset on every mutation. A later update may publish the same key;
the compacted registry retains its latest value eventually, not immediately.

For each change, the producer enqueues ChangeEvent to `config.events` and
ConfigSnapshot to `config.snapshots`. Both use the validated asset ID as UTF-8 key.
Ordering applies **only within a partition of a topic**. There is no cross-topic
atomicity or ordering, including the asset registry; separate consumers may
observe records in a different order. Never depend on event-before-snapshot or
snapshot-after-event arrival. Future enrichment must handle a missing asset row.

`config.snapshots` is the primary Spark configuration-processing stream.
`config.events` is for audit, event analytics, triggers, and operational history;
it is not a necessary ordered precursor to processing a snapshot.

The producer enables Kafka idempotence and `acks=all`, but generator state is
in-memory and advances before delivery confirmation. Failures produce a nonzero
exit; successful process completion requires all queued deliveries to be confirmed.
Replay is deterministic but a new producer/run can append duplicates. No durable
generator checkpoint, transactions, or exactly-once end-to-end guarantee exists.

Compose runs one combined KRaft controller/broker with replication factor 1,
automatic topic creation disabled, loopback-only plaintext exposure, and persistent
Docker volume storage. `make kafka-topics` owns topic initialization/verification.
This is a local development topology without HA or authentication. See
[topic configuration, retention, and local commands](kafka-topics.md).

## Implemented Spark consumer

Host-local Spark 4.0.2 uses `readStream.format("kafka")` to subscribe exclusively
to `config.snapshots`. No Python Kafka consumer feeds the processing path. SQL
`from_json` and a separate StructType parse the existing JSON envelope. SQL
expressions validate basic fields, UTC timestamps, content-location exclusivity,
and Kafka key equality; no per-row Python/Pydantic UDF is involved.

The classified streaming DataFrame retains snapshot content and Kafka provenance.
A valid-snapshot projection exposes the clean transport fields and metadata.
One checkpointed `writeStream.format("console")` projects safe columns only and
labels each row valid/invalid with a fixed diagnostic reason. This is one query,
not two independently advancing readers; malformed records remain visible without
logging raw configuration. This diagnostic classification is not a DLQ producer.

Checkpointed query progress supports local restart. Starting offsets apply only
without prior progress; Kafka retention/deletion can still prevent recovery. A
console display is not a durable or exactly-once sink. Spark's native progress
metrics are logged without claiming measured throughput. See
[version matrix, launch commands, and validation scope](spark-streaming.md).

## Component responsibilities

| Component | Responsibility and rationale |
| --- | --- |
| Sources and collector | Obtain configuration; generate IDs, versions, timestamps; publish records without processing business rules |
| Kafka | Decouple ingestion from compute; retain an ordered, replayable log per partition |
| Spark Structured Streaming | Implemented parsing and basic validation; planned normalization, stateful comparison, rules, and event-time aggregations |
| MinIO + Parquet | S3-compatible local historical source of truth; columnar data for analytical reads |
| Elasticsearch | Derived, rebuildable serving/search indexes; not the authoritative historical store |
| Spark batch | Read historical Parquet for frequency, severity, and anomaly analytics |
| FastAPI | Thin query layer over Elasticsearch; no duplicate diff or rule logic |

## Streaming path

Collectors publish `ConfigSnapshot` and `ChangeEvent` records separately.
The initialized Kafka topics are `config.assets`, `config.snapshots`, `config.events`,
`config.alerts`, and `config.dlq`; only the first three have producers. Use
**asset_id as the Kafka key** whenever per-asset ordering
matters. A single development broker is sufficient. Kafka ordering is within a
partition, not global and not across topics. Producers must coordinate version
assignment for the same asset; repartitioning and retries need explicit handling.

Spark now parses and validates snapshots to console diagnostics. Future stages
will route malformed/unsupported records to the DLQ, deduplicate IDs, normalize
content, compare consecutive snapshots per asset,
evaluate YAML rules, and calculate window metrics. The DLQ must preserve the
original payload, ingestion timestamp, and reason under controlled access.
Spark owns this state and computation; the collector and API do not emulate it.

`event_time` means when a source observed the change. `ingest_time` means when the
collector accepted it; neither is Spark processing time. Both must carry a
timezone. Event-time watermarks will bound state and tolerated lateness. Delayed,
duplicate, and out-of-order records require tested policies before adding sinks.
Arrival order alone is not enough to identify the correct preceding version.

Console query checkpointing is implemented; idempotent document identifiers are
planned. Multiple sinks are not automatically one atomic transaction; replay and partial-write recovery
must be designed and tested rather than advertised as end-to-end exactly-once.

## Storage and batch path

Planned logical locations under the `config-lake` bucket:

- `raw/`: accepted original payloads and ingestion context, access restricted.
- `processed/`: normalized snapshots, diffs, events, and alerts in Parquet.
- `analytics/`: Spark batch reports and derived aggregate results.

Processed snapshots should be partitioned by UTC event date (`year`, `month`,
`day`) and `asset_type`, not `asset_id`. This supports date/type pruning without
creating one tiny partition per asset. File sizes and compaction need later
measurement. Asset type/metadata enrichment will require an explicit reference
data join or collection envelope; snapshots reference an asset by ID rather than
embedding vendor-specific fields.

Elasticsearch will hold `assets`, `snapshots`, `config-events`, `alerts`, and
`metrics`. Large raw configuration blobs belong in the lake, not duplicated in
search indexes. Batch jobs read the lake independently of the serving layer,
calculating changes by asset/hour/day/type, severity counts, frequently changing
assets, and later rolling mean/standard-deviation/z-score anomalies.

## Boundaries and security

- Asset metadata is extensible; a network device is one asset type, not the schema.
- Snapshot content can be inline initially and referenced by `content_uri` later.
- Phase 1 verifies inline raw-content hashes and local record consistency. It
  neither reads URI content nor checks cross-record references, deduplicates IDs,
  or enforces sequential versions. Those operations need producer/processor state.
- Raw configuration may contain credentials. It is never safe merely because it
  passes schema validation. Mask before serving; restrict raw lake/DLQ access.
- Schemas must not log payloads on validation failure without sanitization.
- Configuration comes from environment variables. `.env.example` contains no secrets.
- Docker Compose is the first deployment target; Kubernetes and cloud are deferred.

## Incremental acceptance gates

1. Bootstrap installs and tests on Python 3.12.
2. All five versioned domain contracts validate and JSON round-trip.
3. Stateful generator produces meaningful consecutive versions on the console.
4. Kafka consumer CLI observes keyed messages; Spark then parses them to console.
5. Normalization, diff, rules, event-time state, and replay behavior are tested.
6. Lake and serving sinks preserve history and expose results through FastAPI.

The MVP scenario changes `logging enabled` to `logging disabled`, preserves both
versions in the lake, and exposes a diff plus a high-severity `logging-disabled`
alert. That scenario is a future integration acceptance test, not implemented now.
