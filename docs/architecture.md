# Architecture

## Scope and status

ConfigStream focuses on distributed configuration-change storage and processing.
Phases 0–2 supply a Python project, tested versioned schemas, and a stateful
synthetic generator with JSONL output. The generator maintains independent asset
histories without external services; it does not evaluate rules or compute diffs.
The distributed components below remain the target architecture. Do not add
Docker infrastructure, Kubernetes, or monitoring before it is useful.

## Implemented synthetic source

`generator/engine.py` maintains a list of per-asset states and selects an asset
using a local seeded RNG. Type-specific templates and mutation domains are data;
the mutation and versioning logic is shared across all asset types. Each mutation
constructs the existing ChangeEvent and ConfigSnapshot models, then advances only
the selected state. `generator/serialization.py` emits two JSONL envelopes. CLI
pacing and file/stdout I/O live in `generator/main.py`, outside the engine.

This boundary lets a future collector consume the same model pairs without
duplicating generation or changing external schemas. No Kafka client or other
service dependency exists yet. See [generator lifecycle and timing](generator.md).

## Component responsibilities

| Component | Responsibility and rationale |
| --- | --- |
| Sources and collector | Obtain configuration; generate IDs, versions, timestamps; publish records without processing business rules |
| Kafka | Decouple ingestion from compute; retain an ordered, replayable log per partition |
| Spark Structured Streaming | Distributed parsing, validation, normalization, stateful comparison, rules, and event-time aggregations |
| MinIO + Parquet | S3-compatible local historical source of truth; columnar data for analytical reads |
| Elasticsearch | Derived, rebuildable serving/search indexes; not the authoritative historical store |
| Spark batch | Read historical Parquet for frequency, severity, and anomaly analytics |
| FastAPI | Thin query layer over Elasticsearch; no duplicate diff or rule logic |

## Streaming path

Collectors publish `ConfigSnapshot` and `ChangeEvent` records separately.
The planned Kafka topics are `config.snapshots`, `config.events`, `config.alerts`,
and `config.dlq`. Use **asset_id as the Kafka key** whenever per-asset ordering
matters. A single development broker is sufficient. Kafka ordering is within a
partition, not global and not across topics. Producers must coordinate version
assignment for the same asset; repartitioning and retries need explicit handling.

Spark will parse and validate records, route malformed/unsupported records to the
DLQ, deduplicate IDs, normalize content, compare consecutive snapshots per asset,
evaluate YAML rules, and calculate window metrics. The DLQ must preserve the
original payload, ingestion timestamp, and reason under controlled access.
Spark owns this state and computation; the collector and API do not emulate it.

`event_time` means when a source observed the change. `ingest_time` means when the
collector accepted it; neither is Spark processing time. Both must carry a
timezone. Event-time watermarks will bound state and tolerated lateness. Delayed,
duplicate, and out-of-order records require tested policies before adding sinks.
Arrival order alone is not enough to identify the correct preceding version.

Checkpointing and idempotent document identifiers are planned. Multiple sinks
are not automatically one atomic transaction; replay and partial-write recovery
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
