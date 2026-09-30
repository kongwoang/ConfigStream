# Data model

Phase 1 implements five Pydantic contracts exported by `schemas/__init__.py`.
The models validate data at construction or `model_validate[_json]` boundaries;
they are not a database, collector, diff algorithm, or streaming processor.

## Shared conventions

- All five entities inherit `schema_version: Literal["1.0"]`, defaulting to `1.0`.
  Omission is interpreted as v1.0 for this initial contract. Producers must include
  defaults when serializing, so the version is explicit on the wire.
- Unknown fields and unsupported schema versions are rejected, not silently dropped.
- Identifiers, labels, and messages are non-empty strings after trimming surrounding
  whitespace. They are not restricted to UUIDs. Producers supply stable IDs and
  must use the validated `asset_id` as the Kafka key.
- Timestamps require timezone-aware values, are converted to UTC, and serialize
  as ISO 8601 strings. `event_time` and `ingest_time` are required on snapshots and
  change events: deserialization must not silently invent their times.
- `created_at` on Asset/Alert defaults to current UTC time; provide it when replaying
  a historical record. Models do not require ingestion to follow event time: clock
  skew, delayed collection, and historical replay are legitimate scenarios.
- Metadata values must be JSON-compatible, including nested objects/lists; NaN,
  infinity, and arbitrary Python objects are rejected. Mutable defaults are independent.
- Integer versions/counters are strict: booleans, strings, and floats are rejected.
- Post-construction in-place mutation and `model_copy(update=...)` are not validation
  boundaries. Revalidate reconstructed records before publishing.

## Asset

Source: `schemas/asset.py`.

| Field | Type / default | Meaning |
| --- | --- | --- |
| `schema_version` | `"1.0"` | Contract version |
| `asset_id` | Non-empty string, required | Stable identity of the configuration owner |
| `asset_type` | Non-empty string, required | Extensible category, not a closed enum |
| `metadata` | JSON object, `{}` | Vendor, site, owner, or domain-specific attributes |
| `tags` | List of non-empty strings, `[]` | Search/filter labels |
| `created_at` | UTC timestamp, now | Asset record creation time |

Examples of asset type: `network_device`, `nginx_server`, `linux_server`,
`application`, `kubernetes_resource`, or a future custom type. Vendor/site are
metadata, not top-level schema fields:

```json
{
  "schema_version": "1.0",
  "asset_id": "router-hn-001",
  "asset_type": "network_device",
  "metadata": {"vendor": "cisco", "site": "hanoi"},
  "tags": ["core", "production"],
  "created_at": "2026-09-30T03:00:00Z"
}
```

## ConfigSnapshot

Source: `schemas/snapshot.py`.

| Field | Type / default | Meaning |
| --- | --- | --- |
| `schema_version` | `"1.0"` | Contract version |
| `snapshot_id` | Non-empty string, required | Stable identity for replay/deduplication |
| `asset_id` | Non-empty string, required | Referenced asset |
| `version` | Integer >= 1, required | Producer-assigned per-asset sequence |
| `config_format` | Non-empty string, `"text"` | Extensible format label; not a vendor parser |
| `event_time` | UTC timestamp, required | Time configuration was observed at source |
| `ingest_time` | UTC timestamp, required | Time collector accepted the snapshot |
| `hash` | 64 lowercase hexadecimal characters, required | SHA-256 of original UTF-8 content |
| `content` | String or null, null | Inline configuration, preserved byte-for-byte after UTF-8 encoding |
| `content_uri` | URI or null, null | Reference to externally stored content |

Exactly one of `content` and `content_uri` must be non-null. Empty string content
is valid and uses the SHA-256 of empty bytes. Inline hashes are checked; whitespace,
newlines, and Unicode are not normalized first. This is a raw-content integrity
hash, not a normalized-content hash. Secret-only changes can change this hash even
when the future normalized diff is empty. `config_format` does not validate the
syntax of the configuration itself.

References support `s3://bucket/path` and `https://host/path` with a non-root object
path. Credentials, query strings (including presigned tokens), fragments, relative
paths, `file://`, and plaintext HTTP are rejected. Storage credentials belong in
runtime environment configuration, not a persisted URI. The model does not fetch
the object or verify a URI-only snapshot's hash. A future resolver must verify the
retrieved bytes and enforce a configured bucket/host allowlist before reading.

Moving content to storage preserves `snapshot_id`, `asset_id`, version, timestamps,
and hash. Set `content` to null and `content_uri` to the object location. The caller
must ensure the object is immutable/durably written before publishing the reference.
Asset type is obtained from Asset metadata/enrichment later, not inferred from IDs.

## ChangeEvent

Source: `schemas/event.py`.

| Field | Type / default | Meaning |
| --- | --- | --- |
| `schema_version` | `"1.0"` | Contract version |
| `event_id` | Non-empty string, required | Stable event identity |
| `asset_id` | Non-empty string, required | Referenced asset |
| `event_type` | Non-empty string, `"config_changed"` | Extensible fact/event type |
| `source` | Non-empty string, required | E.g. synthetic, file, or SSH |
| `event_time` | UTC timestamp, required | Source observation time |
| `ingest_time` | UTC timestamp, required | Collector acceptance time |
| `metadata` | JSON object, `{}` | Context; may include a correlating `snapshot_id` |

Events describe facts, not mutable application database rows. An event and its
snapshot are separate records; correlation is not an atomic cross-topic guarantee.

## ConfigDiff

Source: `schemas/diff.py`; `DiffLine` is an embedded detail model, not a separately
versioned event. Its version is governed by the containing ConfigDiff contract.

| Field | Type / default | Meaning |
| --- | --- | --- |
| `schema_version` | `"1.0"` | Contract version |
| `diff_id` | Non-empty string, required | Diff identity |
| `asset_id` | Non-empty string, required | Referenced asset |
| `old_snapshot_id` | Non-empty string or null, null | Previous version; null denotes initial baseline |
| `new_snapshot_id` | Non-empty string, required | Current snapshot, different from previous ID |
| `added` | Integer >= 0, required | Total inserted lines, including replacement new lines |
| `removed` | Integer >= 0, required | Total deleted lines, including replacement old lines |
| `changed` | Integer >= 0, `0` | Paired replacement lines, a subset of added/removed |
| `details` | List of DiffLine, `[]` | Ordered `operation` (`added`/`removed`) and literal `line` |

Summary counts must agree with detail operations. `changed` cannot exceed
`min(added, removed)`. A replaced line counts as one added, one removed, and one
changed; do not sum all three as disjoint totals. The future diff engine determines
replacement pairing; the schema cannot infer it from line text alone.

No-change diffs have zero counts and an empty list. Baseline diffs cannot remove
lines. Line text preserves whitespace and permits blank lines for later display.
For the logging example, the detail list is:

```json
[
  {"operation": "removed", "line": "logging enabled"},
  {"operation": "added", "line": "logging disabled"}
]
```

## Alert

Source: `schemas/alert.py`.

| Field | Type / default | Meaning |
| --- | --- | --- |
| `schema_version` | `"1.0"` | Contract version |
| `alert_id` | Non-empty string, required | Stable alert identity |
| `asset_id` | Non-empty string, required | Referenced asset |
| `snapshot_id` | Non-empty string, required | Snapshot which caused the alert |
| `rule_id` | Non-empty string, required | Originating rule identifier |
| `severity` | Required enum | `info`, `low`, `medium`, `high`, `critical` |
| `message` | Non-empty string, required | Human-readable explanation |
| `created_at` | UTC timestamp, now | Rule evaluation/alert creation time |

Severity is case-sensitive. For `logging-disabled`, the intended later rule emits
severity `high` and message `Logging was disabled`. Phase 1 validates that alert
shape; it does not evaluate any rule.

## Evolution, safety, and limits

Schema version is separate from snapshot version. Unknown versions/fields fail
validation intentionally. Even additive top-level fields need a versioned contract
and coordinated consumer rollout because v1.0 forbids extras. Extensible metadata
can carry new optional domain attributes without changing existing field meanings.
Preserve historical schema versions; introduce explicit migration/upcasting when
needed. Future ingestion can route unsupported messages to `config.dlq`.

The models do not enforce ID uniqueness, asset existence, cross-asset reference
integrity, monotonic versions, or message ordering. Those require state in later
producer and Spark phases. No secret masking happens here. Snapshot content is
excluded from its normal repr and validation error strings hide input values, but
JSON dumps and structured validation errors may still contain sensitive payloads.
Never log them indiscriminately or treat validation as sanitization.
