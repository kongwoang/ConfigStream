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

**Phases 0 and 1 complete.** Python packaging, local development tools, documented
architecture, and tested Pydantic contracts for Asset, ConfigSnapshot, ChangeEvent,
ConfigDiff, and Alert are available. No generator, Kafka, Spark, storage service,
API, dashboard, or deployment is implemented yet. Phase 2 is intentionally deferred.

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
| `make clean` | Remove test/build outputs, not datasets or environments |

`.env.example` documents safe configuration placeholders. No runtime application
settings exist yet and `.env` is not automatically loaded. Future services will
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

The `synthetic` source label illustrates the contract; a producer does not exist
yet. See [data-model semantics and limitations](docs/data-model.md) before writing
producers or consumers. Tests exercise invalid inputs, generic assets, JSON
round-trips, timestamp handling, content locations/hashes, diff consistency, and
severity values. They are schema tests, not pipeline integration or performance tests.

## Initial roadmap

| Phase | Deliverable |
| --- | --- |
| 0 — complete | Bootstrap, architecture, tooling, packaging smoke test |
| 1 — complete | Pydantic Asset, ConfigSnapshot, ChangeEvent, ConfigDiff, Alert and tests |
| 2 | Stateful, seeded synthetic workload generator with console output |
| 3–4 | Single-broker Kafka in Docker Compose; Spark console consumer |
| 5–6 | Normalization, diff, YAML rules, event-time state, deduplication, metrics |
| 7–9 | MinIO/Parquet, Elasticsearch, and thin FastAPI: end-to-end MVP |
| 10 | Historical Spark analytics and simple frequency anomalies |
| 11–13 | Optional dashboard, SSH/FRR adapters, then Kubernetes deployment |

Python 3.12, Pydantic, pytest, and Ruff are the initial stack. Kafka, PySpark,
MinIO, Parquet, Elasticsearch, and FastAPI are planned, not current dependencies.
Each phase is tested, committed, and pushed before the next begins. No throughput,
latency, fault-tolerance, or exactly-once claims are made before experiments.
