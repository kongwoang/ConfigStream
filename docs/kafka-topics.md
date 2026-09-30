# Kafka ingestion and asset registry

## Implemented scope

Phase 3 publishes Asset, ChangeEvent, and ConfigSnapshot using the optional
`confluent-kafka` Python client (librdkafka). Its asynchronous callbacks, built-in
producer retries/idempotence, and bounded queue interface fit a workload source
without putting Kafka internals into generator state. `make setup` installs the
locked Kafka extra; a base `configstream` install remains sufficient for JSONL.

All five topics below are explicitly initialized, with **replication factor 1**.
Only assets/events/snapshots have producers. Alert evaluation, DLQ routing, and
Spark are **not implemented**. RF=1 and a combined broker/controller are local
development choices, not a production deployment.

## Topic contracts

| Topic | Partitions | Key | Value payload | Producer now | Future consumer / purpose | Cleanup / retention |
| --- | --- | --- | --- | --- | --- | --- |
| `config.assets` | 3 | `asset_id` | Asset | Workload startup; updates via producer wrapper | Compacted reference registry; metadata enrichment | `compact`, `retention.ms=-1` |
| `config.events` | 6 | `asset_id` | ChangeEvent | Each generator mutation | Audit, event analytics, operational history, triggers | `delete`, 7 days |
| `config.snapshots` | 6 | `asset_id` | ConfigSnapshot | Each generator mutation | **Primary future Spark configuration stream** | `delete`, 7 days |
| `config.alerts` | 3 | `asset_id` (planned) | Alert (planned) | None | Processing output / alert consumers | `delete`, 7 days |
| `config.dlq` | 3 | Original key when known (planned) | Original payload + error + ingest time (not implemented) | None | Invalid/unprocessable-record inspection and replay | `delete`, 7 days |

Partition counts and cleanup/retention settings have one source of truth in
`messaging/topics.py`. `make kafka-topics` creates missing topics and verifies
existing topic partition counts, RF, and config. Re-running is safe when settings
match. A mismatch fails instead of silently resizing, deleting, or changing data.
Kafka auto topic creation is disabled at both broker and producer.

Compaction is asynchronous: several records for the same asset key can coexist.
It eventually retains the latest value for each key, rather than preserving full
asset history. `retention.ms=-1` is not a promise that all earlier versions survive
compaction. Asset updates republish the key; tombstone deletion is not supported
by the Phase 3 wrapper. The other topics' 7-day retention is segment-based and
asynchronous, not an exact per-message deletion deadline. Kafka is not yet backed
by a data lake; do not treat development topic retention as archival storage.

## Keys, envelopes, and ordering

The wrapper accepts an existing Pydantic model, selects its topic, and passes
`record.asset_id.encode("utf-8")` as key. It leaves partition selection to Kafka's
client partitioner, keeping an asset on one partition **within each topic** while
partition count/partitioner stay unchanged. Do not casually increase partition
counts: existing key histories can then span partitions.

Every UTF-8 JSON value uses the same envelope as JSONL:

```json
{"record_type":"asset","payload":{"schema_version":"1.0","asset_id":"router-000001","asset_type":"network_device","metadata":{"source":"synthetic"},"tags":["synthetic"],"created_at":"2026-09-30T00:00:00Z"}}
```

Other active `record_type` values are `change_event` and `config_snapshot`.
`schema_version` remains inside the payload. Serialization uses sorted compact
JSON and one trailing newline in both transports; configuration newlines are
escaped. JSONL change output remains byte-identical to Phase 2. No Avro, Protobuf,
Schema Registry, or new external schema was introduced.

Kafka mode publishes N Asset records once at startup and flushes that batch before
generating workload changes. It then enqueues event followed by snapshot for each
mutation. This call order is **not a cross-topic delivery or consumption guarantee**.
Ordering is only within one partition of one topic; it is not global and not
across topics. An independently scheduled snapshot consumer can observe a snapshot
before the corresponding event or before its asset-registry consumer is caught up.
Future Spark must primarily consume `config.snapshots`, not wait for an event
trigger that supposedly arrived first. Correlation uses existing asset/snapshot IDs.

## Producer configuration and failure boundary

| Setting | Value / environment variable |
| --- | --- |
| Bootstrap servers | `KAFKA_BOOTSTRAP_SERVERS`, default `localhost:9092` |
| Client ID | `KAFKA_CLIENT_ID`, default `configstream-generator` |
| Address family | IPv4, matching the loopback-only development port binding |
| Idempotence | `enable.idempotence=true` |
| Acknowledgements | `acks=all` (fixed to preserve idempotence configuration) |
| In-flight requests per connection | 5 |
| Linger | 5 ms |
| Delivery deadline | `KAFKA_DELIVERY_TIMEOUT_MS`, default 10000 |
| Flush deadline | `KAFKA_FLUSH_TIMEOUT_SECONDS`, default 15 |
| Queue-full wait budget | `KAFKA_ENQUEUE_TIMEOUT_SECONDS`, default 5 |

Environment parsing is centralized in `messaging/config.py`. Python does not read
`.env` automatically: export values in your shell. Compose may read `.env` for its
own interpolation; that does not export variables to the Python process. Changing
a client bootstrap address also does not rewrite the broker's advertised listeners.
No credentials are needed for this localhost-only plaintext example.

`publish(record)` polls callbacks before and after enqueueing. Buffer-full errors
cause bounded polling/retry, not a per-message flush or silent drop. Async delivery
errors are logged and retained; the next publish/flush fails. Synchronous errors,
queue timeout, nonzero pending-message count after flush, and failed callbacks
produce a nonzero process exit. Logs use Python logging/stderr without payloads.

The producer flushes at the asset-batch boundary and on context exit, including
normal interruption/error unwinding. A signal or hard kill can still prevent
cleanup. Success is logged only after final flush confirms all records. A failure
can leave a partially published workload; no automatic whole-workload retry exists.

Generator state advances on generation, not acknowledgement. Repeating the same
arguments reproduces the same keys, payloads, IDs, and simulated timestamps, but
Kafka can append them again in a new producer session. Producer idempotence is
enabled and its configuration is unit-tested; successful publishing is integration-
tested. Retry failover/duplicate suppression under fault injection is not measured.
There is **no exactly-once end-to-end claim**, transaction, atomic event/snapshot
pair, durable generator checkpoint, or broker HA. `acks=all` with RF=1 still means
one broker copy only.

## Local broker and platform support

`docker-compose.yml` pins `apache/kafka:4.1.2`, using KRaft without ZooKeeper.
The image manifest provides Linux arm64 and amd64 variants; no amd64 emulation is
forced. Apple Silicon was tested; Linux amd64 is supported by the image but not
claimed as locally exercised here. Allocate room for a JVM broker: Compose caps
it at 1536 MiB with a 512 MiB heap. Startup waits on an actual broker API health check.

- Host clients: `localhost:9092`, published only at `127.0.0.1:9092`.
- Clients on the Compose network: `kafka:29092` (INTERNAL listener).
- Controller: `kafka:9093`, not published to the host.
- Data: Compose's `configstream_kafka-data` volume, not a repository folder.

A running Docker Engine/Compose is a prerequisite, not installed by `make setup`.
No Kubernetes or cloud resources are involved. `make kafka-down` preserves data.
For an intentional **destructive local reset only**, `docker compose down --volumes`
removes this project's broker data; do not use that command on data you need.

## Commands and acceptance

```sh
make setup
make check
make kafka-up
make kafka-topics
make kafka-topics
make kafka-demo
make kafka-status
make kafka-consume-assets KAFKA_CONSUME_MAX_MESSAGES=3
make kafka-consume-events KAFKA_CONSUME_MAX_MESSAGES=8
make kafka-consume-snapshots KAFKA_CONSUME_MAX_MESSAGES=8
make integration-test
make kafka-down
```

`make kafka-demo` runs exactly:

```sh
.venv/bin/python -m generator.main --assets 3 --events 8 --rate 100 --seed 42 --no-sleep --sink kafka
```

On a fresh broker this produces 3 assets, 8 events, and 8 snapshots; later runs
append data (asset compaction happens eventually). The three `kafka-consume-*`
commands default to one message, print `key<TAB>value`, and time out rather than
wait forever if not enough records exist. They invoke the real Kafka CLI:

```sh
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic config.snapshots \
  --from-beginning --max-messages 8 --timeout-ms 10000 --property print.key=true
```

Example asset line (the gap between key and JSON is a tab):

```text
router-000001	{"payload":{"asset_id":"router-000001","asset_type":"network_device","created_at":"2026-09-30T00:00:00Z","metadata":{"source":"synthetic"},"schema_version":"1.0","tags":["synthetic"]},"record_type":"asset"}
```

To inspect end offsets per partition on a fresh, non-transactional topic:

```sh
docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka:29092 --topic 'config\.(assets|events|snapshots)' --time -1
```

End offsets are log positions, not retained-record counts after compaction/deletion.
They can be summed as counts for a fresh demo starting at offset zero, but not as
a general-purpose asset-count metric.

## Tests and measured acceptance

`make test` / `make check` run only fast unit tests, with Kafka clients mocked;
Docker is unnecessary. `make integration-test` (equivalently
`.venv/bin/python -m pytest -m integration tests/integration`) needs a healthy broker
and initialized topics. Missing/unreachable Kafka fails integration tests instead
of silently skipping them. Tests use high-watermark offsets before publishing,
so prior records do not affect counts. They assume no concurrent writers to these
development topics, do not delete topics, and may leave small test records.

The integration suite verifies topic metadata/config, real CLI publication,
3/8/8 counts from the current workload, schema-valid payloads, asset keys, stable
per-asset topic partitioning, and a nonzero exit when the broker is unavailable.
It does not assert event-before-snapshot consumption across topics. No production
Python consumer is introduced; the test consumer is confined to integration tests.

Local acceptance on macOS arm64 used Docker Engine 29.8.1, Compose 5.5.1, Kafka
4.1.2, and Python 3.12.14. Kafka CLI captured 3 assets, 8 events, and 8 snapshots;
all decoded payloads validated against existing schemas and every key equaled
`asset_id`. JSONL retained the Phase 2 demo SHA-256
`ae9fb7b3b347c069822fd1a208d85573b0ddad959971b63aadd439af75db0af2`.
These are correctness checks, not throughput measurements.

## References

- [Apache Kafka Docker quickstart](https://kafka.apache.org/41/getting-started/docker/)
- [Confluent Python client API](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html)
