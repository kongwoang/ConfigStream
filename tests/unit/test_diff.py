from typing import Any

import pytest
from pydantic import ValidationError

from schemas import ConfigDiff, DiffLine


def test_changed_lines_are_preserved_for_display(diff_payload: dict[str, Any]) -> None:
    diff = ConfigDiff.model_validate(diff_payload)
    assert (diff.added, diff.removed, diff.changed) == (1, 1, 1)
    assert diff.details == [
        DiffLine(operation="removed", line="logging enabled"),
        DiffLine(operation="added", line="logging disabled"),
    ]


def test_unchanged_config_has_zero_counts(diff_payload: dict[str, Any]) -> None:
    diff_payload.update(added=0, removed=0, changed=0, details=[])
    assert ConfigDiff.model_validate(diff_payload).details == []


def test_baseline_diff_has_no_previous_snapshot(diff_payload: dict[str, Any]) -> None:
    diff_payload.update(old_snapshot_id=None, removed=0, changed=0)
    diff_payload["details"] = [{"operation": "added", "line": "logging enabled"}]
    assert ConfigDiff.model_validate(diff_payload).old_snapshot_id is None


@pytest.mark.parametrize("field_name", ["added", "removed", "changed"])
@pytest.mark.parametrize("value", [-1, True, "1", 1.5])
def test_diff_counts_are_nonnegative_integers(
    diff_payload: dict[str, Any], field_name: str, value: Any
) -> None:
    with pytest.raises(ValidationError):
        ConfigDiff.model_validate({**diff_payload, field_name: value})


@pytest.mark.parametrize(
    "fields",
    [
        {"added": 2},
        {"removed": 2},
        {"changed": 2},
        {"old_snapshot_id": "snap-011"},
        {"old_snapshot_id": None},
        {"details": [{"operation": "unknown", "line": "logging enabled"}]},
        {"details": [{"operation": "added", "line": "new", "unexpected": True}]},
    ],
)
def test_inconsistent_diffs_are_rejected(
    diff_payload: dict[str, Any], fields: dict[str, Any]
) -> None:
    with pytest.raises(ValidationError):
        ConfigDiff.model_validate({**diff_payload, **fields})


def test_detail_line_preserves_whitespace() -> None:
    assert DiffLine(operation="added", line="  timeout 30  ").line == "  timeout 30  "
    assert DiffLine(operation="removed", line="").line == ""
