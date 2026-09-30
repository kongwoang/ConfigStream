import json

import pytest

from generator.engine import WorkloadGenerator
from generator.serialization import serialize_change, serialize_record
from schemas import Asset, ChangeEvent, ConfigSnapshot


def test_jsonl_has_exactly_two_schema_valid_envelopes_per_change() -> None:
    event, snapshot = WorkloadGenerator(assets=1).next_change()
    encoded = serialize_change(event, snapshot)
    lines = encoded.splitlines()
    assert encoded.endswith("\n")
    assert len(lines) == 2
    event_envelope, snapshot_envelope = map(json.loads, lines)
    assert set(event_envelope) == set(snapshot_envelope) == {"record_type", "payload"}
    assert event_envelope["record_type"] == "change_event"
    assert snapshot_envelope["record_type"] == "config_snapshot"
    assert ChangeEvent.model_validate(event_envelope["payload"]) == event
    assert ConfigSnapshot.model_validate(snapshot_envelope["payload"]) == snapshot


def test_serialization_escapes_newlines_and_preserves_unicode() -> None:
    event, _ = WorkloadGenerator(assets=1).next_change()
    event.metadata["description"] = "máy chủ\nconfiguration"
    encoded = serialize_record(event)
    assert len(encoded.splitlines()) == 1
    assert "máy chủ" in encoded
    assert json.loads(encoded)["payload"]["metadata"]["description"] == "máy chủ\nconfiguration"


def test_unsupported_record_type_is_rejected() -> None:
    with pytest.raises(TypeError, match="only ChangeEvent"):
        serialize_record(Asset(asset_id="asset-001", asset_type="application"))
