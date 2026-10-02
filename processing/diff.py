import json
from difflib import SequenceMatcher
from itertools import zip_longest
from uuid import NAMESPACE_URL, uuid5

from processing.errors import AssetMismatchError, InvalidVersionOrderError, SnapshotIdentityError
from processing.models import DiffResult, NormalizedConfig
from processing.normalization import normalize_snapshot
from schemas import ConfigDiff, ConfigSnapshot, DiffLine

_DIFF_POLICY = "configstream:text-diff:v1"


def _validate_pair(
    previous: ConfigSnapshot | NormalizedConfig,
    current: ConfigSnapshot | NormalizedConfig,
) -> None:
    identity = (
        f"previous {previous.snapshot_id!r} (asset {previous.asset_id!r}, v{previous.version}), "
        f"current {current.snapshot_id!r} (asset {current.asset_id!r}, v{current.version})"
    )
    if previous.asset_id != current.asset_id:
        raise AssetMismatchError(f"Snapshots must belong to the same asset: {identity}")
    if current.version <= previous.version:
        raise InvalidVersionOrderError(f"Current version must be greater than previous: {identity}")
    if previous.snapshot_id == current.snapshot_id:
        raise SnapshotIdentityError(f"Snapshot identifiers must differ: {identity}")


def _lines(content: str) -> list[str]:
    return content.removesuffix("\n").split("\n") if content else []


def _diff_id(previous: NormalizedConfig, current: NormalizedConfig) -> str:
    identity = json.dumps(
        [
            _DIFF_POLICY,
            previous.asset_id,
            previous.snapshot_id,
            previous.version,
            previous.normalized_hash,
            current.snapshot_id,
            current.version,
            current.normalized_hash,
        ],
        ensure_ascii=True,
        separators=(",", ":"),
    )
    return f"diff-{uuid5(NAMESPACE_URL, identity)}"


def compare_snapshots(previous: NormalizedConfig, current: NormalizedConfig) -> DiffResult:
    """Compare an explicit, increasing same-asset pair; no arrival-order inference."""
    _validate_pair(previous, current)
    details: list[DiffLine] = []
    changed = 0
    if previous.normalized_content != current.normalized_content:
        old_lines = _lines(previous.normalized_content)
        new_lines = _lines(current.normalized_content)
        matcher = SequenceMatcher(None, old_lines, new_lines, autojunk=False)
        for operation, old_start, old_end, new_start, new_end in matcher.get_opcodes():
            if operation == "equal":
                continue
            removed = old_lines[old_start:old_end]
            added = new_lines[new_start:new_end]
            if operation == "replace":
                changed += min(len(removed), len(added))
            for old_line, new_line in zip_longest(removed, added):
                if old_line is not None:
                    details.append(DiffLine(operation="removed", line=old_line))
                if new_line is not None:
                    details.append(DiffLine(operation="added", line=new_line))
    diff = ConfigDiff(
        diff_id=_diff_id(previous, current),
        asset_id=current.asset_id,
        old_snapshot_id=previous.snapshot_id,
        new_snapshot_id=current.snapshot_id,
        added=sum(detail.operation == "added" for detail in details),
        removed=sum(detail.operation == "removed" for detail in details),
        changed=changed,
        details=details,
    )
    return DiffResult(
        diff=diff,
        version_gap=current.version - previous.version - 1,
        old_normalized_hash=previous.normalized_hash,
        new_normalized_hash=current.normalized_hash,
    )


def process_snapshot_pair(previous: ConfigSnapshot, current: ConfigSnapshot) -> DiffResult:
    """Normalize each inline snapshot once, then compare without modifying either."""
    _validate_pair(previous, current)
    return compare_snapshots(normalize_snapshot(previous), normalize_snapshot(current))
