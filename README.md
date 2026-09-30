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

**Phase 0: repository bootstrap.** Python packaging, local development tools, a
packaging smoke test, and architecture/data-model drafts are available. Domain
schemas are the next milestone. No generator, Kafka, Spark, storage service, API,
dashboard, or deployment is implemented yet.

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

## Initial roadmap

| Phase | Deliverable |
| --- | --- |
| 0 | Bootstrap, architecture, tooling, packaging smoke test |
| 1 | Pydantic Asset, ConfigSnapshot, ChangeEvent, ConfigDiff, Alert and tests |
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
