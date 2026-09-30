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

**Phases 0, 1, and 2 complete.** Python packaging, local development tools, documented
architecture, and tested Pydantic contracts for Asset, ConfigSnapshot, ChangeEvent,
ConfigDiff, and Alert are available, together with a deterministic stateful
synthetic generator producing JSON Lines. Kafka, Spark, storage services, API,
dashboard, and deployments are not implemented. Phase 3 is intentionally deferred.

## Architecture overview (target, not yet implemented)

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
and internet access for the initial setup. No Docker services are needed yet.

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

| Command | Purpose |
| --- | --- |
| `make setup` | Install the locked development environment |
| `make test` | Run local tests without external services |
| `make lint` | Check Ruff lint and formatting |
| `make format` | Apply Ruff fixes and formatting |
| `make check` | Run lint, formatting checks, and tests |
| `make generator` | Print 10 changes (20 JSONL records) across 5 assets, seed 42, without sleeping |
| `make clean` | Remove test/build outputs, not datasets or environments |

`.env.example` documents safe configuration placeholders. No service environment
variables are needed yet; generator settings use CLI arguments. `.env` is not
automatically loaded. Future services will
use environment variables; never commit secrets, generated datasets, or service
state. Keep generated data in the ignored project-local directories.

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
| 3–4 | Single-broker Kafka in Docker Compose; Spark console consumer |
| 5–6 | Normalization, diff, YAML rules, event-time state, deduplication, metrics |
| 7–9 | MinIO/Parquet, Elasticsearch, and thin FastAPI: end-to-end MVP |
| 10 | Historical Spark analytics and simple frequency anomalies |
| 11–13 | Optional dashboard, SSH/FRR adapters, then Kubernetes deployment |

Python 3.12, Pydantic, pytest, and Ruff are the initial stack. Kafka, PySpark,
MinIO, Parquet, Elasticsearch, and FastAPI are planned, not current dependencies.
Each phase is tested, committed, and pushed before the next begins. No throughput,
latency, fault-tolerance, or exactly-once claims are made before experiments.
