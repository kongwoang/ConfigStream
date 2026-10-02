# Configuration processing (Phase 5)

## Scope and API

The pure Python `processing` package normalizes inline text and compares an
**explicit pair** of snapshots. It needs only the existing schemas and Python
standard library: no Kafka, Spark, Java, network access, or new dependencies.
The Phase 4 Spark consumer still only parses/validates and prints safe metadata.
It does **not** call this package or maintain previous-version state yet.

```python
from processing import compare_snapshots, normalize_snapshot, process_snapshot_pair

previous_normalized = normalize_snapshot(previous)
current_normalized = normalize_snapshot(current)
result = compare_snapshots(previous_normalized, current_normalized)
diff = result.diff
version_gap = result.version_gap

equivalent_result = process_snapshot_pair(previous, current)
```

Here `previous` and `current` are validated `schemas.ConfigSnapshot` instances.
Use either API style, not both when processing the same pair. The explicit
normalized form lets a future caller reuse a normalized snapshot between pairs.
Neither function mutates its inputs. Pydantic's unvalidated mutation/copy APIs are
not an input validation boundary; callers must provide validated snapshots.

`NormalizedConfig` is an internal frozen dataclass containing `asset_id`,
`snapshot_id`, `version`, `normalized_content`, and a computed `normalized_hash`.
Use `normalize_snapshot` to construct it. It is not a new transport schema or
asset registry. `DiffResult` contains the existing `ConfigDiff`, `version_gap`,
and the old/new normalized hashes. **No public schema was changed.**

## Exact normalization policy

`normalize_text(raw)` is deterministic and idempotent. Policy v1:

1. Convert CRLF and lone CR to LF.
2. Remove trailing ASCII spaces/tabs on every line.
3. Remove leading/trailing blank lines; collapse interior blank runs to one
   blank line. A single interior blank line remains significant.
4. Remove only whole lines matching these metadata conventions, case-insensitively,
   with optional indentation and whitespace after the comment marker:
   - `# generated at <nonempty text>`
   - `# last modified: <nonempty text>`
   - `! generated` or `! generated <text>`
   Ordinary comments and inline comments remain, except on masked secret settings.
   A source must not use these reserved prefixes for meaningful configuration.
5. For a simple initial setting name (`[A-Za-z_][A-Za-z0-9_.-]*`), canonicalize its
   separator: whitespace to one space, ` = ` to `=`, and ` : ` to `: `.
   For non-secret names, colon assignment requires whitespace after the colon,
   so a bare `https://...` is not rewritten. Spaces/tabs around `=` are optional.
6. Mask recognized secret assignments as described below.
7. Preserve line order, leading indentation, key case, Unicode, and spacing
   *within* values (including quoted strings). Do not sort or vendor-parse.
8. End nonempty output with exactly one LF; return `""` for empty output.

This is a deliberately narrow line-oriented policy, not universal semantic
equivalence. Even whitespace or comment conventions can carry meaning in some
languages; use a format-specific parser before applying this policy to such input.
There is no runtime plugin/configuration system for normalization rules in Phase 5.

## Secrets and privacy tradeoff

Exact initial setting names, case-insensitive:

`password`, `passwd`, `secret`, `token`, `api_key`, `api-key`.

Whitespace, `=`, and `:` assignment forms are supported, including indented
settings. Preserve the setting name/case, indentation, and delimiter kind; replace
the **entire right-hand side** with `***`. Quote wrappers and trailing comments on
that right-hand side are intentionally removed rather than risking value leakage:

```text
password <synthetic value>        → password ***
API_KEY = "<synthetic value>"    → API_KEY=***
  token: <synthetic value>       →   token: ***
```

Already-masked values remain masked. `password_file`, `tokenizer`, and an embedded
`description token ...` are not recognized secret settings. This is **not a
complete credential detector**: nested/vendor-specific commands, arbitrary
comments, PEM blocks, embedded URLs, and unknown names may still contain secrets.
Do not treat a normalized config as automatically safe for unrestricted logging.

For recognized secret keys, obvious unsupported multiline syntax fails closed:
a trailing backslash, triple-quoted value, unclosed initial single/double quote,
or `|`/`>` block marker (including simple modifiers/comment suffix). It raises
`UnsupportedSecretSyntaxError` rather than leaving continuation lines visible.
This guard is not a general multiline parser or a guarantee for unknown syntax.

Secret-value rotation intentionally produces **no normalized diff**:
`password old-secret` and `password new-secret` both become `password ***`.
The original snapshots and raw hashes still differ and are left untouched. The
planned raw history layer must preserve both source versions under restricted
access; Phase 5 itself does not persist history. Future rules must not depend on
secret values, and secret-rotation auditing would need a separate protected design.

Core functions do not log configurations. Content is excluded from normalized
object reprs; diff content is excluded from the result wrapper's repr. Exceptions
contain only the condition and relevant asset/snapshot IDs and versions, never
configuration content or content URIs. Explicitly dumping `result.diff` still
prints changed lines; only the synthetic demo does this by default.

## Hashes and identity

- `ConfigSnapshot.hash` is SHA-256 of **original UTF-8 bytes**, already checked by
  the schema for inline content. It represents source integrity.
- `NormalizedConfig.normalized_hash` is SHA-256 of **normalized UTF-8 content**,
  including its canonical terminal LF. It is computed at construction, not passed
  independently, and remains reproducible for a fixed normalization policy.
- Noise-only or recognized secret-value-only changes can have different raw hashes
  but equal normalized hashes/content. Visible semantic changes affect the latter.
  The diff's no-change check compares content, not just hashes; hashes are neither
  encryption nor a universal proof of semantic equivalence.
- Diff IDs are UUID5-derived from the fixed `configstream:text-diff:v1` policy
  label, asset ID, both snapshot IDs/versions, and both normalized hashes. No clock,
  random UUID4, or process hash seed is used. Identical pairs reproduce IDs/details;
  a different pair still has a distinct identity even if its content is unchanged.
  Policy changes must deliberately version this identity contract.

## Pair selection and versions

The caller must select the previous version **for the same asset**. Neither Kafka
arrival order, global generator order, nor timestamp order identifies that version.
The function has no global state and does not search history.

| Pair | Behavior |
| --- | --- |
| Same asset, v1 → v2 | Allowed; `version_gap=0` |
| Same asset, v1 → v3 | Allowed; `version_gap=1` |
| Same asset, v2 → v2 | `InvalidVersionOrderError` |
| Same asset, v3 → v2 | `InvalidVersionOrderError` |
| Different assets | `AssetMismatchError` |
| Same snapshot ID despite increasing version | `SnapshotIdentityError` |

`version_gap = current.version - previous.version - 1`: the number of unseen
intermediate versions. The diff compares only the supplied endpoints, not the
cumulative changes inside the gap. A v1 → v3 diff can be empty despite an unknown
temporary v2 change. Gap diagnostics live in `DiffResult`, not an invented field
inside the existing ConfigDiff schema.

This API requires two snapshots; first-snapshot/baseline policy is left to a later
caller even though the schema permits `old_snapshot_id=null`.

## Diff algorithm and counts

Python's [difflib.SequenceMatcher](https://docs.python.org/3.12/library/difflib.html)
compares ordered normalized LF-separated lines with `autojunk=False`, so frequent
configuration lines are not treated as popularity noise. It is a deterministic
line-based comparison, not a vendor-aware semantic matcher or a guaranteed
minimal edit script; worst-case cost can be quadratic. No performance claims are
made here.

- `added`: every inserted line, including the new side of replacements.
- `removed`: every deleted line, including the old side of replacements.
- `changed`: sum of `min(old_block_length, new_block_length)` **per replace
  opcode**. Independent insertion/deletion blocks do not count as replacements.
  It is a subset, not a third disjoint counter: never sum all three.
- Details preserve opcode order. Within replacements, positional old/new lines
  are interleaved (`removed`, `added`); leftover lines remain one-sided.
  Pairing is presentational, not an assertion that two lines are the same setting.
- Equal normalized content returns zero counts and `details=[]`, with a valid,
  deterministic ConfigDiff. Reordering is not silently erased.

## Acceptance demo and tests

```sh
make setup
make check
make processing-demo
.venv/bin/python -m pytest tests/unit/test_normalization.py tests/unit/test_config_diff_engine.py
```

`make processing-demo` constructs router-001 v10/v11 using synthetic password
values and generated comments. It prints only normalized inputs, both normalized
hashes, the readable diff, and the full schema-valid ConfigDiff:

```text
Previous normalized (v10):
hostname router-001
logging enabled
telnet disabled
password ***
SHA-256: b84f1e3142578ba72cf3aee21821ca3a1ba40cd189c6d59cd91812ee1f4fb611

Current normalized (v11):
hostname router-001
logging disabled
telnet enabled
password ***
SHA-256: 72e996cf48ab853485ab3b7ba768484b2468d4dd5459ed51e8ae94d4674b8c22

Version gap: 0
- logging enabled
+ logging disabled
- telnet disabled
+ telnet enabled
```

The resulting ConfigDiff has `added=2`, `removed=2`, `changed=2`,
`old_snapshot_id=snap-router-001-v10`, `new_snapshot_id=snap-router-001-v11`,
and `diff_id=diff-5109e22e-8349-532a-a3d3-410c5937db4e`. There are no password or
generated-comment details.

Tests cover normalization idempotence, exact masking boundaries, safe failures,
hash invariants, line changes, counts, ordering, version gaps/rejections, schema
round-trips, and deterministic demo output across processes/hash seeds. They also
compare all three generator asset types, including logging, telnet, DNS, timeout,
replicas, and nginx access_log changes, and successive per-asset generator snapshots.
Core tests import neither PySpark nor the Kafka client.

## Limits and Phase 6 boundary

Only exact `config_format="text"` with inline content is accepted. Other labels
raise `UnsupportedConfigFormatError`; URI-only content raises
`ContentUnavailableError`. No fetcher or JSON/YAML semantic parser is implemented.

Phase 6 must design per-asset version state, duplicate and late-event policies,
bounded state/watermarks, checkpoint/replay behavior, and then rules/alerts and
window metrics. It can reuse these functions after establishing an explicit pair.
The choice of Spark stateful execution and Python overhead needs measurement;
no Python UDF, Spark adapter, distributed state, or scalability claim is added here.
