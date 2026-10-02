from collections.abc import Callable
from hashlib import sha256
from typing import Any

import pytest

from schemas import ConfigSnapshot


@pytest.fixture
def snapshot_factory(snapshot_payload: dict[str, Any]) -> Callable[..., ConfigSnapshot]:
    def create(raw_content: str, **overrides: Any) -> ConfigSnapshot:
        return ConfigSnapshot.model_validate(
            {
                **snapshot_payload,
                "content": raw_content,
                "hash": sha256(raw_content.encode("utf-8")).hexdigest(),
                **overrides,
            }
        )

    return create


@pytest.fixture
def snapshot_payload() -> dict[str, Any]:
    content = "hostname router-001\nlogging enabled\n"
    return {
        "snapshot_id": "snap-001",
        "asset_id": "router-001",
        "version": 1,
        "event_time": "2026-09-30T10:00:00+07:00",
        "ingest_time": "2026-09-30T03:00:01Z",
        "hash": sha256(content.encode("utf-8")).hexdigest(),
        "content": content,
    }


@pytest.fixture
def event_payload() -> dict[str, Any]:
    return {
        "event_id": "evt-001",
        "asset_id": "router-001",
        "source": "synthetic",
        "event_time": "2026-09-30T03:00:00Z",
        "ingest_time": "2026-09-30T03:00:01Z",
    }


@pytest.fixture
def diff_payload() -> dict[str, Any]:
    return {
        "diff_id": "diff-001",
        "asset_id": "router-001",
        "old_snapshot_id": "snap-010",
        "new_snapshot_id": "snap-011",
        "added": 1,
        "removed": 1,
        "changed": 1,
        "details": [
            {"operation": "removed", "line": "logging enabled"},
            {"operation": "added", "line": "logging disabled"},
        ],
    }


@pytest.fixture
def alert_payload() -> dict[str, Any]:
    return {
        "alert_id": "alert-001",
        "asset_id": "router-001",
        "snapshot_id": "snap-011",
        "rule_id": "logging-disabled",
        "severity": "high",
        "message": "Logging was disabled",
    }
