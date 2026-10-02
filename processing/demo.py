from datetime import UTC, datetime
from hashlib import sha256

from processing.diff import compare_snapshots
from processing.normalization import normalize_snapshot
from schemas import ConfigSnapshot


def demo_snapshots() -> tuple[ConfigSnapshot, ConfigSnapshot]:
    """Build synthetic inputs; only their normalized forms may be printed."""
    snapshots = []
    for version, logging, telnet, password, minute in (
        (10, "enabled", "disabled", "old-secret", 0),
        (11, "disabled", "enabled", "new-secret", 1),
    ):
        content = (
            f"hostname router-001\nlogging {logging}\ntelnet {telnet}\n"
            f"password {password}\n# generated at 10:{minute:02d}\n"
        )
        snapshots.append(
            ConfigSnapshot(
                snapshot_id=f"snap-router-001-v{version}",
                asset_id="router-001",
                version=version,
                event_time=datetime(2026, 10, 1, 10, minute, tzinfo=UTC),
                ingest_time=datetime(2026, 10, 1, 10, minute, 1, tzinfo=UTC),
                hash=sha256(content.encode("utf-8")).hexdigest(),
                content=content,
            )
        )
    return snapshots[0], snapshots[1]


def main() -> None:
    previous, current = (normalize_snapshot(snapshot) for snapshot in demo_snapshots())
    result = compare_snapshots(previous, current)
    for label, normalized in (("Previous", previous), ("Current", current)):
        print(f"{label} normalized (v{normalized.version}):")
        print(normalized.normalized_content, end="")
        print(f"SHA-256: {normalized.normalized_hash}\n")
    print(f"Version gap: {result.version_gap}")
    for detail in result.diff.details:
        prefix = "+" if detail.operation == "added" else "-"
        print(f"{prefix} {detail.line}")
    print("\nConfigDiff:")
    print(result.diff.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
