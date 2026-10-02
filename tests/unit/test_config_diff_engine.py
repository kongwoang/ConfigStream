import json
import os
import subprocess
import sys
from collections.abc import Callable
from hashlib import sha256

import pytest

from generator.engine import WorkloadGenerator
from generator.templates import TEMPLATES, initial_config, render_config
from processing import compare_snapshots, normalize_snapshot, process_snapshot_pair
from processing.demo import demo_snapshots
from processing.errors import AssetMismatchError, InvalidVersionOrderError, SnapshotIdentityError
from schemas import ConfigDiff, ConfigSnapshot


@pytest.mark.parametrize(
    ("old", "new", "counts", "details"),
    [
        ("", "", (0, 0, 0), []),
        ("logging enabled", "logging enabled", (0, 0, 0), []),
        ("", "timeout 30", (1, 0, 0), [("added", "timeout 30")]),
        ("timeout 30", "", (0, 1, 0), [("removed", "timeout 30")]),
        (
            "logging enabled",
            "logging enabled\ntimeout 30",
            (1, 0, 0),
            [("added", "timeout 30")],
        ),
        (
            "logging enabled\ntimeout 30",
            "logging enabled",
            (0, 1, 0),
            [("removed", "timeout 30")],
        ),
        (
            "logging enabled",
            "logging disabled",
            (1, 1, 1),
            [("removed", "logging enabled"), ("added", "logging disabled")],
        ),
        (
            "logging enabled\ntelnet disabled",
            "logging disabled\ntelnet enabled",
            (2, 2, 2),
            [
                ("removed", "logging enabled"),
                ("added", "logging disabled"),
                ("removed", "telnet disabled"),
                ("added", "telnet enabled"),
            ],
        ),
        (
            "logging enabled",
            "logging disabled\ntimeout 30",
            (2, 1, 1),
            [
                ("removed", "logging enabled"),
                ("added", "logging disabled"),
                ("added", "timeout 30"),
            ],
        ),
        (
            "logging enabled\ntimeout 30",
            "logging disabled",
            (1, 2, 1),
            [
                ("removed", "logging enabled"),
                ("added", "logging disabled"),
                ("removed", "timeout 30"),
            ],
        ),
        (
            "remove 1\nanchor 2\nkeep 3",
            "anchor 2\nkeep 3\nadd 4",
            (1, 1, 0),
            [("removed", "remove 1"), ("added", "add 4")],
        ),
        (
            "timeout 30\n\nreplicas 2",
            "timeout 30\nreplicas 2",
            (0, 1, 0),
            [("removed", "")],
        ),
        (
            "alpha 1\nbeta 2",
            "beta 2\nalpha 1",
            (1, 1, 0),
            [("added", "beta 2"), ("removed", "beta 2")],
        ),
        (
            "timeout 30\nstable 1\nreplicas 2",
            "timeout 60\nstable 1\nreplicas 3",
            (2, 2, 2),
            [
                ("removed", "timeout 30"),
                ("added", "timeout 60"),
                ("removed", "replicas 2"),
                ("added", "replicas 3"),
            ],
        ),
    ],
)
def test_diff_details_and_counts(
    snapshot_factory: Callable[..., ConfigSnapshot],
    old: str,
    new: str,
    counts: tuple[int, int, int],
    details: list[tuple[str, str]],
) -> None:
    previous = snapshot_factory(old)
    current = snapshot_factory(new, snapshot_id="snap-002", version=2)
    result = process_snapshot_pair(previous, current)
    diff = result.diff
    assert (diff.added, diff.removed, diff.changed) == counts
    assert [(detail.operation, detail.line) for detail in diff.details] == details
    assert (diff.asset_id, diff.old_snapshot_id, diff.new_snapshot_id) == (
        current.asset_id,
        previous.snapshot_id,
        current.snapshot_id,
    )
    assert result.version_gap == 0
    assert result == compare_snapshots(normalize_snapshot(previous), normalize_snapshot(current))
    assert ConfigDiff.model_validate_json(diff.model_dump_json()) == diff


@pytest.mark.parametrize("noise", ["whitespace", "comment", "secret"])
def test_ignored_changes_produce_empty_diff(
    snapshot_factory: Callable[..., ConfigSnapshot], noise: str
) -> None:
    content = "logging enabled\npassword synthetic-before\n# generated at 10:00\n"
    altered = {
        "whitespace": "\r\n" + content.replace("logging ", "logging   ").replace("\n", " \r\n"),
        "comment": content.replace("10:00", "10:01"),
        "secret": content.replace("synthetic-before", "synthetic-after"),
    }[noise]
    previous = snapshot_factory(content)
    current = snapshot_factory(altered, snapshot_id="snap-002", version=2)
    result = process_snapshot_pair(previous, current)
    assert previous.hash != current.hash
    assert result.old_normalized_hash == result.new_normalized_hash
    assert (result.diff.added, result.diff.removed, result.diff.changed) == (0, 0, 0)
    assert result.diff.details == []


@pytest.mark.parametrize(
    ("old_version", "new_version", "gap"),
    [(1, 2, 0), (1, 3, 1), (10, 20, 9)],
)
def test_version_gap(
    snapshot_factory: Callable[..., ConfigSnapshot], old_version: int, new_version: int, gap: int
) -> None:
    previous = snapshot_factory("logging enabled", version=old_version)
    current = snapshot_factory("logging disabled", snapshot_id="snap-002", version=new_version)
    result = process_snapshot_pair(previous, current)
    assert result.version_gap == gap
    assert "version_gap" not in result.diff.model_dump()


@pytest.mark.parametrize(("previous_version", "current_version"), [(2, 2), (3, 2)])
@pytest.mark.parametrize("normalized", [False, True])
def test_rejects_duplicate_or_reverse_version(
    snapshot_factory: Callable[..., ConfigSnapshot],
    previous_version: int,
    current_version: int,
    normalized: bool,
) -> None:
    previous = snapshot_factory("logging enabled", version=previous_version)
    current = snapshot_factory("logging disabled", snapshot_id="snap-002", version=current_version)
    with pytest.raises(InvalidVersionOrderError, match="snap-001.*snap-002"):
        if normalized:
            compare_snapshots(normalize_snapshot(previous), normalize_snapshot(current))
        else:
            process_snapshot_pair(previous, current)


@pytest.mark.parametrize("normalized", [False, True])
def test_rejects_cross_asset(
    snapshot_factory: Callable[..., ConfigSnapshot], normalized: bool
) -> None:
    previous = snapshot_factory("logging enabled")
    current = snapshot_factory(
        "logging disabled", asset_id="nginx-002", snapshot_id="snap-002", version=2
    )
    with pytest.raises(AssetMismatchError, match="router-001.*nginx-002"):
        if normalized:
            compare_snapshots(normalize_snapshot(previous), normalize_snapshot(current))
        else:
            process_snapshot_pair(previous, current)


def test_rejects_reused_snapshot_identity(snapshot_factory: Callable[..., ConfigSnapshot]) -> None:
    previous = snapshot_factory("logging enabled")
    current = snapshot_factory("logging disabled", version=2)
    with pytest.raises(SnapshotIdentityError, match="snap-001"):
        process_snapshot_pair(previous, current)


def test_pair_errors_do_not_expose_config(snapshot_factory: Callable[..., ConfigSnapshot]) -> None:
    previous = snapshot_factory("unrecognized synthetic-private-value", version=2)
    current = snapshot_factory("unrecognized synthetic-private-value", snapshot_id="snap-002")
    with pytest.raises(InvalidVersionOrderError) as error:
        process_snapshot_pair(previous, current)
    assert "synthetic-private-value" not in str(error.value)


def test_pair_validation_precedes_normalization(
    snapshot_factory: Callable[..., ConfigSnapshot],
) -> None:
    previous = snapshot_factory("logging enabled", config_format="yaml")
    current = snapshot_factory("logging disabled", snapshot_id="snap-002", asset_id="other")
    with pytest.raises(AssetMismatchError):
        process_snapshot_pair(previous, current)


def test_pair_does_not_use_timestamp_order(snapshot_factory: Callable[..., ConfigSnapshot]) -> None:
    previous = snapshot_factory("logging enabled", event_time="2026-10-02T00:00:00Z")
    current = snapshot_factory(
        "logging disabled", version=2, snapshot_id="snap-002", event_time="2026-10-01T00:00:00Z"
    )
    assert process_snapshot_pair(previous, current).diff.changed == 1


def test_repeated_lines_are_not_discarded(snapshot_factory: Callable[..., ConfigSnapshot]) -> None:
    content = "feature enabled\n" * 250
    previous = snapshot_factory(content)
    current = snapshot_factory(content + "feature enabled\n", snapshot_id="snap-002", version=2)
    diff = process_snapshot_pair(previous, current).diff
    assert (diff.added, diff.removed, diff.changed) == (1, 0, 0)
    assert diff.details[0].line == "feature enabled"


def test_diff_is_deterministic_and_pair_specific(
    snapshot_factory: Callable[..., ConfigSnapshot],
) -> None:
    previous = snapshot_factory("logging enabled")
    current = snapshot_factory("logging disabled", version=2, snapshot_id="snap-002")
    original = (previous.model_dump(), current.model_dump())
    first = process_snapshot_pair(previous, current)
    assert first == process_snapshot_pair(previous, current)
    another = snapshot_factory("logging disabled", version=3, snapshot_id="snap-003")
    assert first.diff.diff_id != process_snapshot_pair(previous, another).diff.diff_id
    altered = snapshot_factory("logging enabled", version=2, snapshot_id="snap-002")
    assert first.diff.diff_id != process_snapshot_pair(previous, altered).diff.diff_id
    assert (previous.model_dump(), current.model_dump()) == original


@pytest.mark.parametrize(
    ("asset_type", "setting", "new_value"),
    [
        ("network_device", "logging", "disabled"),
        ("network_device", "telnet", "enabled"),
        ("network_device", "dns", "1.1.1.1"),
        ("generic_service", "timeout", "60"),
        ("generic_service", "replicas", "3"),
        ("nginx_server", "access_log", "disabled"),
    ],
)
def test_generator_templates(
    snapshot_factory: Callable[..., ConfigSnapshot],
    asset_type: str,
    setting: str,
    new_value: str,
) -> None:
    config = initial_config(asset_type, "demo-001")
    previous = snapshot_factory(render_config(config))
    old_value = config[setting]
    config[setting] = new_value
    current = snapshot_factory(render_config(config), snapshot_id="snap-002", version=2)
    result = process_snapshot_pair(previous, current)
    assert result.old_normalized_hash != result.new_normalized_hash
    assert (result.diff.added, result.diff.removed, result.diff.changed) == (1, 1, 1)
    assert [(detail.operation, detail.line) for detail in result.diff.details] == [
        ("removed", f"{setting} {old_value}"),
        ("added", f"{setting} {new_value}"),
    ]


def test_successive_generator_snapshots_remain_asset_local() -> None:
    generator = WorkloadGenerator(assets=3, seed=42)
    previous_by_asset: dict[str, ConfigSnapshot] = {}
    compared_types = set()
    for event, current in generator.generate(60):
        previous = previous_by_asset.get(current.asset_id)
        if previous is not None:
            assert event.metadata["previous_snapshot_id"] == previous.snapshot_id
            result = process_snapshot_pair(previous, current)
            assert result.version_gap == 0
            assert result.diff.old_snapshot_id == previous.snapshot_id
            assert result.diff.new_snapshot_id == current.snapshot_id
            assert result.diff.asset_id == current.asset_id
            assert result.diff.added + result.diff.removed > 0
            assert result.old_normalized_hash != result.new_normalized_hash
            compared_types.add(event.metadata["asset_type"])
        previous_by_asset[current.asset_id] = current
    assert compared_types == set(TEMPLATES)
    assert len(previous_by_asset) == 3


def test_demo_model_and_safe_output() -> None:
    previous, current = demo_snapshots()
    result = process_snapshot_pair(previous, current)
    assert (previous.version, current.version, result.version_gap) == (10, 11, 0)
    assert (result.diff.added, result.diff.removed, result.diff.changed) == (2, 2, 2)
    outputs = [
        subprocess.run(
            [sys.executable, "-m", "processing.demo"],
            check=True,
            capture_output=True,
            env={**os.environ, "PYTHONHASHSEED": hash_seed},
        ).stdout
        for hash_seed in ("1", "2")
    ]
    assert sha256(outputs[0]).digest() == sha256(outputs[1]).digest()
    for value in (b"old-secret", b"new-secret", b"generated at"):
        assert value not in outputs[0]
    assert b"password ***" in outputs[0]
    diff_json = outputs[0].decode().split("ConfigDiff:\n", 1)[1]
    assert ConfigDiff.model_validate(json.loads(diff_json)) == result.diff


def test_processing_import_does_not_load_optional_services() -> None:
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import processing; "
            "assert not any(name.split('.')[0] in {'pyspark', 'confluent_kafka'} "
            "for name in sys.modules)",
        ],
        check=True,
        capture_output=True,
    )
