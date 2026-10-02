# Stateful synthetic workload generator

The generator introduced in Phase 2 is a single-process synthetic source. It reuses `schemas.Asset`,
`schemas.ChangeEvent`, and `schemas.ConfigSnapshot` without modifying or duplicating
their contracts. JSON Lines remains the default, service-free mode. Phase 3 adds
an optional Kafka sink and startup Asset registry without changing the generated
change histories. See [Kafka setup and topic contract](kafka-topics.md).

## Run

After `make setup`, use `.venv/bin/python` or activate `.venv` and use `python`:

```sh
make generator
.venv/bin/python -m generator.main --assets 3 --events 8 --seed 42
.venv/bin/python -m generator.main --assets 100 --events 1000 --rate 100 --seed 42 --no-sleep
.venv/bin/python -m generator.main --assets 1000 --duration 30 --rate 100 --seed 42 --no-sleep --output datasets/sample.jsonl
.venv/bin/python -m generator.main --assets 10 --events 20 --asset-mix nginx_server,generic_service --start-time 2026-10-01T07:00:00+07:00 --no-sleep
.venv/bin/python -m generator.main --help
```

| Argument | Semantics / default |
| --- | --- |
| `--assets` | Positive count, default 100; all states initialized in memory |
| `--events` | Nonnegative number of configuration mutations; zero emits nothing |
| `--duration` | Positive finite simulated seconds; emits `floor(duration * rate)` mutations |
| `--rate` | Integer changes/second, default 100, range 1–1,000,000 |
| `--seed` | Integer RNG seed, default 42; negative seeds are also accepted |
| `--start-time` | Timezone-aware ISO 8601, default `2026-09-30T00:00:00Z` |
| `--asset-mix` | Ordered comma-separated type list, default `network_device,nginx_server,generic_service` |
| `--output` | New UTF-8 file; default `-` means stdout |
| `--sink` | `jsonl` (default) or `kafka`; file output is JSONL-only |
| `--no-sleep` | Generate as fast as possible; record timestamps remain unchanged |

Exactly one of `--events` and `--duration` is required. Fractional durations are
accepted; fractional rates are not. A duration shorter than `1/rate` emits zero
changes. Diagnostics go to stderr, never into JSONL. File output uses exclusive
creation: existing files are never overwritten or appended. Parent directories
are created when necessary. Use ignored `datasets/` for workloads; `.jsonl` is
also ignored globally. No output file is created by default, and `make clean`
does not delete datasets.

## Asset lifecycle and versioning

1. Assign each asset a type by cycling through `asset_mix`. Repeating a type gives
   it more slots: `network_device,network_device,generic_service` yields roughly
   a 2:1 mix (exact counts depend on the asset count). IDs use the type prefix and
   a global one-based ordinal, such as `router-000001`, `nginx-000002`, and
   `service-000003`. This assignment is deterministic, not sampled.
2. Create a distinct Asset and template dictionary per asset. Asset `created_at`
   equals the configured start time. Internal state starts at version **0**, with
   no last snapshot ID or event time. Asset records and version-0 templates are
   not emitted in JSONL mode. Kafka publishes one Asset per initialized asset
   before workload changes, but still does not emit version-0 snapshots.
3. Uniformly sample one asset with the generator's private `random.Random` instance.
   Select an applicable mutation and transform a copy of that asset's configuration.
4. Build and validate a ChangeEvent and ConfigSnapshot using existing schemas.
   Increment only that asset's version, starting with emitted version **1**; update
   its current configuration, last snapshot ID, and last event time.
5. Emit the event followed by its snapshot. Future changes reference the selected
   asset's preceding emitted snapshot, not the preceding global message.

The first event's `previous_snapshot_id` is null. Every later event for that asset
points to its preceding emitted snapshot. No ID refers to an un-emitted bootstrap
snapshot. Assets not selected by a short workload remain at internal version 0;
random selection does not guarantee every asset appears in the output. A first
snapshot is the first observable baseline for future consumers; reconstructing
the un-emitted template is not required of downstream systems.

`AssetState` is an internal dataclass, not an external schema. The engine keeps a
list for O(1) random asset selection and only current per-asset state, not all past
events. Memory is O(number of assets). Configuration size stays bounded by the
small, fixed required/optional setting domains. No per-change scan of all assets
is performed. Callers must not mutate template definitions or engine state.

## Templates and mutations

Configuration is an ordered mapping of setting names to string values, rendered
as `setting value\n`. Some network setting names contain spaces. Templates are
deliberately simplified synthetic notation, **not deployable vendor or nginx
configuration syntax**. Logical validity means unique settings, preserved required
keys/identity, supported feature states, and values from the template's domain.

All types share these mutation categories:

| Mutation | Behavior |
| --- | --- |
| `change_scalar` | Replace an existing scalar/optional value with a different permitted value |
| `enable_feature` | Change an existing disabled feature to enabled |
| `disable_feature` | Change an existing enabled feature to disabled |
| `add_setting` | Add one absent optional setting |
| `remove_setting` | Remove one present optional setting; never remove required keys |

- Network: change DNS; toggle logging, SSH, telnet, or eth0; add/remove a route,
  interface description, or operator account; change existing optional values.
- Nginx: change worker count/keepalive timeout; toggle access/error logging or gzip;
  add/remove/change body-size and connection limits.
- Generic service: change timeout/replicas; toggle logging/debug; add/remove/change
  retry limits and cache TTL.

Candidates are filtered to exclude no-ops before selection. One applicable
category is selected uniformly, then one eligible `(setting, new value)` pair.
There is no unbounded retry loop. Required template fields are never removed.
Optional fields have fixed names, so long runs cannot accumulate unbounded lines.
Each result carries mutation name, setting, old value, and new value; null means
the setting was absent/removed. No credentials are generated.

## Time and deterministic identifiers

For global zero-based change sequence `sequence`:

```text
event_time  = UTC(start_time) + floor(sequence * 1,000,000 / rate) microseconds
ingest_time = event_time + 5 milliseconds
```

Integer microsecond arithmetic avoids accumulated floating-point timestamp drift.
The maximum rate ensures strictly increasing event times, including per-asset
histories. Ingest time is **simulated collector acceptance**, not the actual
wall-clock output time and not a measured pipeline latency.

CLI pacing uses monotonic absolute deadlines: the first change is immediate, then
change `index` targets `start + index/rate` seconds. Processing/output time counts
toward this schedule. If the sink is slow, the program falls behind instead of
dropping records or pretending the target rate was achieved. Duration fixes the
event count, not wall-clock termination; the last deadline is `(count-1)/rate`.
`--no-sleep` avoids all wall-clock calls in the output loop. The engine itself
never sleeps. Late/out-of-order delivery simulation is intentionally deferred.

UUID5 IDs are derived from a run namespace containing generator contract revision
1, seed, asset count/mix, UTC start time, and rate; each ID additionally includes
record kind, asset ID, per-asset version, and global sequence. Event count, pacing,
and output destination do not enter the namespace. A longer run has the same
prefix as a shorter run, and equivalent timezone representations yield identical
records. Repeating the same run repeats IDs intentionally: useful for replay,
but downstream systems may deduplicate it. Change the seed/start time for a new run.

For the same arguments, implementation, and supported Python 3.12 environment,
serialized output is byte-identical, including IDs, metadata, timestamps, and raw
content hashes. The generator does not touch global random state or wall-clock
timestamps. Reproducibility across future changes to templates, mutation domains,
or Python's sampling implementation is not promised; retain the commit and lockfile.

## JSONL record contract

Each mutation produces exactly two newline-terminated envelopes, in this order:

1. `{"record_type":"change_event","payload":{...}}`
2. `{"record_type":"config_snapshot","payload":{...}}`

These illustrations describe envelope structure; full valid output is available
in the README example and through `make generator`. Compact serialization sorts
JSON keys and escapes configuration newlines, so one physical line is one record.
Payloads include schema version `1.0`; inline content is hashed as UTF-8 SHA-256.

Event metadata includes `mutation`, `setting`, `old_value`, `new_value`,
`snapshot_id`, `previous_snapshot_id`, `asset_type`, and global `sequence`.
Snapshot metadata is not extended: it uses the existing `asset_id`, per-asset
`version`, timestamps, hash, `config_format="text"`, inline content, and null URI.

`messaging/serialization.py` is the shared envelope encoding boundary;
`generator/serialization.py` delegates to it for compatible JSONL output. The
engine returns model pairs through `next_change()` or `generate(count)`; the Kafka
sink publishes those same records without moving mutations into transport code.
State advances when a pair is generated, not after an external acknowledgement.
Output failures/interruption can leave a partial file/pair; no durability,
checkpoint/resume, cross-record atomicity, or delivery guarantee is provided.

## Verification and acceptance demo

```sh
make check
mkdir -p .cache/generator-demo
.venv/bin/python -m generator.main --assets 3 --events 8 --seed 42 > .cache/generator-demo/run1.jsonl
.venv/bin/python -m generator.main --assets 3 --events 8 --seed 42 > .cache/generator-demo/run2.jsonl
cmp .cache/generator-demo/run1.jsonl .cache/generator-demo/run2.jsonl
shasum -a 256 .cache/generator-demo/run1.jsonl .cache/generator-demo/run2.jsonl
```

On the Phase 2 implementation, both files contain 16 records and have SHA-256
`ae9fb7b3b347c069822fd1a208d85573b0ddad959971b63aadd439af75db0af2`.
`service-000003` and `router-000001` each evolve through versions 1–4 independently;
the third asset is initialized but not selected in this short run. Meaningful
changes include timeout 30→15, logging enabled→disabled, telnet disabled→enabled,
DNS changes, adding interface description/cache TTL, and enabling debug.

Automated tests check per-asset state isolation, version/hash/reference invariants,
schema round-trips, every mutation category/type, bounded configuration growth,
same-seed output across separate processes and different `PYTHONHASHSEED` values,
different-seed sequences, UTC conversion, CLI errors/file safety, and pacing with
a fake clock. No unit test waits for workload rate/duration.

## Local scale smoke run

Executed on 2026-09-30, macOS 27.0 arm64, Python 3.12.14: 10,000 assets and 100,000
changes (200,000 JSONL records), seed 42, simulated rate 1,000, without sleeping.
One run took **0.037 s initialization + 2.695 s generation/serialization = 2.732 s**;
process peak RSS was **60,145,664 bytes**. The sink counted 107,574,806 serialized
characters without retaining data. These are single-run local measurements, not
Kafka/Spark throughput, disk/terminal throughput, or a benchmark guarantee.

Reproduce the measurement without creating a large dataset:

```sh
.venv/bin/python - <<'PY'
import io
import platform
import resource
from time import perf_counter

from generator.engine import WorkloadGenerator
from generator.main import write_workload

class CountingOutput(io.TextIOBase):
    characters = 0

    def write(self, text: str) -> int:
        self.characters += len(text)
        return len(text)

started = perf_counter()
generator = WorkloadGenerator(assets=10_000, seed=42, rate=1000)
initialized = perf_counter()
output = CountingOutput()
write_workload(generator, 100_000, output, paced=False)
finished = perf_counter()
assert sum(state.version for state in generator.states) == 100_000
print(platform.platform(), platform.python_version())
print("Initialization seconds:", initialized - started)
print("Generation/serialization seconds:", finished - initialized)
print("Total seconds:", finished - started)
print("Characters serialized:", output.characters)
print("Peak RSS (bytes on macOS, KiB on Linux):", resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
PY
```

## Kafka mode and next boundary

Phase 3 implements a single broker, explicit topics, asset registry, and producer
wrapper. Use `--sink kafka`; no additional Asset lines appear in JSONL mode. Rate
controls changes, not startup asset publication. The registry batch is flushed
before generating changes, and remaining messages are flushed at shutdown. Both
flush timeout and delivery callback errors fail the command. Cross-topic arrival
order and atomicity are not guaranteed. Generator state remains non-durable.

Phase 4 adds a direct Spark consumer for `config.snapshots` while retaining JSONL
as an offline test path. See [Spark parsing and validation](spark-streaming.md).
Phase 5 adds [pure normalization and explicit-pair diff](config-processing.md),
tested with these generator templates and successive per-asset snapshots.
Spark stateful comparison and rules remain future work.
