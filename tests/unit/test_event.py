from typing import Any

import pytest
from pydantic import ValidationError

from schemas import ChangeEvent


def test_event_defaults_and_extensible_types(event_payload: dict[str, Any]) -> None:
    event = ChangeEvent.model_validate(event_payload)
    assert event.event_type == "config_changed"
    assert event.metadata == {}
    custom = ChangeEvent.model_validate({**event_payload, "event_type": "snapshot_collected"})
    assert custom.event_type == "snapshot_collected"


@pytest.mark.parametrize("field_name", ["source", "event_id", "event_type"])
def test_blank_event_fields_are_rejected(event_payload: dict[str, Any], field_name: str) -> None:
    with pytest.raises(ValidationError):
        ChangeEvent.model_validate({**event_payload, field_name: " "})


def test_event_metadata_defaults_are_independent(event_payload: dict[str, Any]) -> None:
    first = ChangeEvent.model_validate(event_payload)
    second = ChangeEvent.model_validate(event_payload)
    first.metadata["snapshot_id"] = "snap-001"
    assert second.metadata == {}


def test_delayed_events_and_clock_skew_are_allowed(event_payload: dict[str, Any]) -> None:
    late = ChangeEvent.model_validate({**event_payload, "ingest_time": "2026-10-01T03:00:00Z"})
    skewed = ChangeEvent.model_validate({**event_payload, "ingest_time": "2026-09-30T02:59:59Z"})
    assert late.ingest_time > late.event_time
    assert skewed.ingest_time < skewed.event_time
