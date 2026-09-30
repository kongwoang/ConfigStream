# Data model (Phase 0 draft)

The next milestone implements these contracts as Pydantic models under `schemas/`.
All externally exchanged entities will carry `schema_version`, starting at `1.0`.
Identifiers are opaque non-empty strings, not tied to vendors or UUID-only syntax.
Timestamps must be timezone-aware; JSON will use ISO 8601 representations.

| Entity | Planned fields and purpose |
| --- | --- |
| Asset | `asset_id`, `asset_type`, extensible `metadata`, `tags`, `created_at` |
| ConfigSnapshot | `snapshot_id`, `asset_id`, positive `version`, `config_format`, `event_time`, `ingest_time`, `hash`, inline `content` or `content_uri` |
| ChangeEvent | `event_id`, `asset_id`, `event_type`, `source`, `event_time`, `ingest_time`, extensible `metadata` |
| ConfigDiff | `diff_id`, `asset_id`, `old_snapshot_id`, `new_snapshot_id`, nonnegative `added`/`removed`/`changed`, displayable `details` |
| Alert | `alert_id`, `asset_id`, `snapshot_id`, `rule_id`, `severity`, `message`, `created_at` |

`asset_type` stays open-ended (`network_device`, `nginx_server`, `application`,
etc.). Vendor and site live inside metadata rather than core vendor-specific fields.
Severity is one of `info`, `low`, `medium`, `high`, or `critical`.

Snapshot version is a per-asset sequence assigned by its producer, independent of
schema version. ID uniqueness, reference integrity, and monotonic version progress
require state outside an individual schema and will be enforced in later phases.
The snapshot contract must allow content to move to object storage without
changing downstream references. Detailed validation and hash/diff semantics will
be specified alongside the Phase 1 implementation.

Unknown schema versions must be rejected deliberately, not silently interpreted
as version 1.0. Evolution requires documented compatibility and updated consumers
before producers publish new versions. Kafka records represent facts/events,
not mutable database rows.
