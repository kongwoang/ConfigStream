# ConfigStream

**Scalable Configuration Change Storage and Analytics Platform**

ConfigStream is a university Big Data Storage and Processing project for storing,
comparing, and analyzing configuration changes over time. Configuration history
should answer not just *what changed*, but *when, how often, and with what risk*.

The central entity is an **Asset**: anything that owns configuration. Network
devices provide the primary demo, but nginx servers, Linux servers, applications,
and Kubernetes resources fit the same model. This is a data-pipeline project, not
a network automation framework or a frontend-first application.

## Current status

**Phases 0, 1, 2, 3, and 4 complete.** Pydantic contracts,
a deterministic stateful generator, JSONL output, and a local single-broker Kafka
path with an explicit asset registry and automated CI are available.
Spark Structured Streaming now reads `config.snapshots`, parses and validates
envelopes, and prints safe metadata with valid/invalid diagnostics.
**Normalization, diff, and rules are NOT implemented yet**, nor are data-lake,
search, API, dashboard, or Kubernetes components.

Implemented Kafka path (all keys are `asset_id`):

```text
Generator
  ├─> config.assets       (startup registry)
  ├─> config.events       (audit / operational history)
  └─> config.snapshots ─> Spark readStream ─> parse / validate ─> console
```

## Architecture overview (long-term target)

1. Stateful synthetic, file, and optional SSH sources feed a separate collector.
2. The collector publishes asset-keyed events and snapshots to Kafka.
3. Spark Structured Streaming validates, deduplicates, normalizes, computes diffs,
   evaluates rules, and aggregates changes using event time.
4. MinIO holds raw and processed Parquet history; Elasticsearch serves searchable
   metadata, events, alerts, and metrics.
5. Spark batch reads historical Parquet for analytics. A thin FastAPI application
   reads Elasticsearch; a minimal dashboard may follow.

See [architecture](docs/architecture.md) and [data model](docs/data-model.md).

## Quick start

Prerequisites: Git, Make, an existing Python 3 installation with `venv` and `pip`,
and internet access for the initial setup. JSONL and unit tests need no services.
Kafka mode additionally needs a running Docker Engine and Docker Compose v2+
(Docker Desktop or another compatible runtime on macOS; Docker Engine on Linux).

```sh
git clone https://github.com/kongwoang/ConfigStream.git
cd ConfigStream
make setup
make check
```

`make setup` installs uv in `.tools/uv`, obtains Python **3.12** if necessary, and
creates `.venv` using committed `uv.lock`. Downloaded Python builds, caches, and
temporary files stay inside this repository (`.tools/`, `.cache/`). It does not
install global tools or edit shell profiles. These generated directories are
ignored by Git. Setup can be repeated safely.

Setup includes the optional `kafka` dependency for the quickstart. The installed
library's base dependencies remain sufficient for JSONL; Kafka imports are lazy.

| Command | Purpose |
| --- | --- |
| `make setup` | Install the locked development environment |
| `make test` | Run local tests without external services |
| `make lint` | Check Ruff lint and formatting |
| `make format` | Apply Ruff fixes and formatting |
| `make check` | Run lint, formatting checks, and tests |
| `make generator` | Print 10 changes (20 JSONL records) across 5 assets, seed 42, without sleeping |
| `make kafka-up` / `make kafka-down` | Start a healthy broker / stop it, preserving its volume |
| `make kafka-topics` | Create missing topics and verify their explicit configuration |
| `make kafka-demo` | Publish 3 assets and 8 changes (19 records total) |
| `make kafka-status` | Show broker status |
| `make integration-test` | Run real Kafka tests, separately from unit tests |
| `make spark-setup` | Install locked Kafka + Spark extras; requires an external Java 21 JDK |
| `make spark-stream` | Read snapshot envelopes into a safe diagnostic console |
| `make spark-test` | Run tiny local[2] Spark SQL tests, without Kafka |
| `make spark-integration-test` | Verify real Kafka → Spark, invalid records, and checkpoint resume |
| `make clean` | Remove test/build outputs, not datasets or environments |

[GitHub Actions CI](https://github.com/kongwoang/ConfigStream/actions/workflows/ci.yml)
runs on pushes to `main` and pull requests, using Python 3.12 on Ubuntu, locked
dependencies, and `make check`. A separate Java 21 job runs `make spark-test`.
Neither job starts Docker or runs Kafka/Spark integration tests. Dependency
downloads are cached. Run service-dependent tests separately as documented below.

`.env.example` documents Kafka environment variables. Defaults work locally;
Python does not automatically load `.env`. Export overrides in your shell, such as
`export KAFKA_BOOTSTRAP_SERVERS=localhost:9092`. Never commit credentials, generated
datasets, or service state. Kafka data is in a Docker-managed named volume, not Git.

## Kafka quickstart

```sh
make setup
make check
make kafka-up
make kafka-topics
make kafka-demo
make kafka-consume-assets KAFKA_CONSUME_MAX_MESSAGES=3
make kafka-consume-events KAFKA_CONSUME_MAX_MESSAGES=8
make kafka-consume-snapshots KAFKA_CONSUME_MAX_MESSAGES=8
make integration-test
make kafka-down
```

The broker is pinned to `apache/kafka:4.1.2`, with KRaft, one node, and an IPv4
loopback listener at `localhost:9092`. Its image supports Linux arm64 and amd64;
the local acceptance run uses Apple Silicon. This plaintext, replication-factor-1
setup is **not production-ready**. It does not install a container runtime for you.

```sh
.venv/bin/python -m generator.main --assets 3 --events 8 --rate 100 --seed 42 --no-sleep --sink kafka
```

Kafka mode sends one Asset per initialized asset, flushes that registry batch,
then sends changes. The three active topics receive **3 / 8 / 8** records for a
fresh demo. Re-running appends records; idempotence does not deduplicate independent
generator runs. Consumer commands print `key<TAB>JSON` and default to one message.

`--sink jsonl` remains the default with byte-compatible Phase 2 output; asset
records are only added in Kafka mode. Both transports use the same JSON envelope
and existing versioned schemas. Kafka logs go to stderr; Kafka mode emits no JSONL
to stdout and does not accept file output.

**Ordering is only within one partition of one topic.** Sending an event before
its snapshot is not a cross-topic delivery/consumption guarantee. Spark
processing consumes `config.snapshots`; `config.events` is an audit
stream, not a required ordered trigger. See [topics and failure semantics](docs/kafka-topics.md).

## Spark streaming quickstart

Install a **Java 21 JDK** separately; `java -version` must report major 21. Set
`JAVA_HOME` if needed. Spark/PySpark is pinned to **4.0.2**, Scala binary **2.13**,
and connector **`org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2`**. The Python
entry point resolves that connector automatically; internet access is needed on
first launch. Kafka stays in Docker; Spark runs on the host, not in a new cluster.

```sh
java -version
make spark-setup
make check
make spark-test
```

Terminal A, from the project root:

```sh
make kafka-up
make kafka-topics
make spark-stream
```

Wait for the first `Progress` log (initial offsets established). In terminal B:

```sh
make kafka-demo
```

Expect **8 valid snapshot rows**, showing asset ID, version, snapshot ID, UTC event
time, hash, and Kafka metadata. Assets/events are not consumed. The single console
query labels invalid rows with `record_status=invalid` and `validation_error`;
neither branch prints configuration content or URIs. The display samples at most
20 rows per batch by default; it is not durable storage or a full audit log.

To exercise malformed JSON, wrong record type, missing asset ID, and key mismatch:

```sh
.venv/bin/python -m scripts.kafka_invalid_demo
make spark-integration-test
```

These are deliberately bad **local test records**. The live query stays running
and shows four diagnostics. Stop it with Ctrl-C, then `make kafka-down`.

Default `SPARK_STARTING_OFFSETS=latest` reads new data on a fresh query. For bounded
replay of retained history, use **a new checkpoint directory**:

```sh
SPARK_STARTING_OFFSETS=earliest \
SPARK_CHECKPOINT_DIR=.cache/spark-checkpoints/replay-example \
make spark-stream SPARK_ARGS=--available-now
```

An existing checkpoint overrides starting offsets; reusing it resumes rather than
replaying. Checkpoints and downloaded JARs are ignored. See [Spark runtime, SQL
validation, progress, and acceptance](docs/spark-streaming.md). `make setup` syncs
only the base/Kafka environment; use `make spark-setup` again before Spark commands.

## Try the schemas

After setup, this example constructs and validates an inline snapshot:

```sh
.venv/bin/python - <<'PY'
from datetime import UTC, datetime
from hashlib import sha256

from schemas import ConfigSnapshot

content = "hostname router-001\nlogging enabled\n"
snapshot = ConfigSnapshot(
    snapshot_id="snap-001",
    asset_id="router-001",
    version=1,
    event_time=datetime(2026, 9, 30, 3, 0, tzinfo=UTC),
    ingest_time=datetime.now(UTC),
    hash=sha256(content.encode("utf-8")).hexdigest(),
    content=content,
)
print(snapshot.model_dump_json(indent=2))
assert ConfigSnapshot.model_validate_json(snapshot.model_dump_json()) == snapshot
PY
```

All five entities emit `schema_version: "1.0"`. Timestamp inputs require a timezone
and are converted to UTC. A snapshot has exactly one of `content` or `content_uri`;
an inline snapshot's SHA-256 is checked against its original UTF-8 content. Empty
configuration is valid. Schema validation does not normalize or mask secrets.

Example change event (independent of the snapshot topic):

```json
{
  "schema_version": "1.0",
  "event_id": "evt-001",
  "asset_id": "router-001",
  "event_type": "config_changed",
  "source": "synthetic",
  "event_time": "2026-09-30T03:00:00Z",
  "ingest_time": "2026-09-30T03:00:01Z",
  "metadata": {"snapshot_id": "snap-001"}
}
```

The Phase 2 generator uses this contract and includes mutation details in event
metadata. See [data-model semantics and limitations](docs/data-model.md) before
writing producers or consumers. Tests exercise invalid inputs, generic assets, JSON
round-trips, timestamp handling, content locations/hashes, diff consistency, and
severity values. They are schema tests, not pipeline integration or performance tests.

## Generate a stateful workload

```sh
make generator
.venv/bin/python -m generator.main --assets 3 --events 8 --seed 42
.venv/bin/python -m generator.main --assets 100 --events 1000 --rate 100 --seed 42 --no-sleep
.venv/bin/python -m generator.main --assets 1000 --duration 30 --rate 100 --seed 42 --no-sleep --output datasets/sample.jsonl
```

Each asset owns a separate configuration and version counter. Mutations transform
its previous state rather than creating unrelated snapshots. Built-in types are
`network_device`, `nginx_server`, and `generic_service`. An internal template is
version 0 (not emitted); the first mutation emits version 1. Later changes link to
the preceding emitted snapshot for that asset.

Every change writes **ChangeEvent first, ConfigSnapshot second**, one envelope per
line. For `--assets 3 --events 8 --seed 42`, the first output line is:

```json
{"payload":{"asset_id":"service-000003","event_id":"evt-73e40c41-486c-596a-ba0a-cd52d958a7dd","event_time":"2026-09-30T00:00:00Z","event_type":"config_changed","ingest_time":"2026-09-30T00:00:00.005000Z","metadata":{"asset_type":"generic_service","mutation":"change_scalar","new_value":"15","old_value":"30","previous_snapshot_id":null,"sequence":0,"setting":"timeout","snapshot_id":"snap-d97a8487-9359-5f8b-840a-1054a6ccb455"},"schema_version":"1.0","source":"synthetic"},"record_type":"change_event"}
```

The following snapshot contains `timeout 15` instead of the template's `timeout 30`.
`--rate` means changes per second, not JSONL lines. Real-time pacing is enabled by
default; `--no-sleep` removes waiting without altering record contents. Duration
produces `floor(duration * rate)` changes, not a nondeterministic wall-clock cutoff.

The same seed and arguments produce byte-identical output, including IDs, hashes,
and timestamps. Defaults are seed **42**, rate **100**, and start time
**2026-09-30T00:00:00Z**; simulated ingestion is 5 ms later. Custom timezone-aware
`--start-time` and comma-separated `--asset-mix` are supported. Stdout is the
default; file output creates parent directories but refuses to overwrite existing
files. Generated `.jsonl` files and `datasets/` are ignored by Git.

See [generator design, CLI, verification, and limitations](docs/generator.md).

## Initial roadmap

| Phase | Deliverable |
| --- | --- |
| 0 — complete | Bootstrap, architecture, tooling, packaging smoke test |
| 1 — complete | Pydantic Asset, ConfigSnapshot, ChangeEvent, ConfigDiff, Alert and tests |
| 2 — complete | Stateful, seeded synthetic workload generator with JSONL output |
| 3 — complete | Single-broker Kafka, asset registry, producer tests, GitHub Actions quality checks |
| 4 — complete | Direct Spark Kafka source, envelope parsing, validation, diagnostics, checkpoints |
| 5–6 | Normalization, diff, YAML rules, event-time state, deduplication, metrics |
| 7–9 | MinIO/Parquet, Elasticsearch, and thin FastAPI: end-to-end MVP |
| 10 | Historical Spark analytics and simple frequency anomalies |
| 11–13 | Optional dashboard, SSH/FRR adapters, then Kubernetes deployment |

Python 3.12, Pydantic, pytest, Ruff, Kafka, optional confluent-kafka, and PySpark 4.0.2
form the current stack. MinIO, Parquet, Elasticsearch, and FastAPI remain planned.
Each phase is tested, committed, and pushed before the next begins. No throughput,
latency, fault-tolerance, or exactly-once claims are made before experiments.
