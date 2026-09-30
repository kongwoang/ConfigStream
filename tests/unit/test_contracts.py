import json
from datetime import UTC, datetime
from typing import Any

import pytest
from pydantic import ValidationError

from schemas import Alert, Asset, ChangeEvent, ConfigDiff, ConfigSnapshot
from schemas.base import VersionedModel


@pytest.fixture(params=["asset", "snapshot", "event", "diff", "alert"])
def contract(
    request: pytest.FixtureRequest,
    snapshot_payload: dict[str, Any],
    event_payload: dict[str, Any],
    diff_payload: dict[str, Any],
    alert_payload: dict[str, Any],
) -> tuple[type[VersionedModel], dict[str, Any]]:
    contracts = {
        "asset": (Asset, {"asset_id": "app-001", "asset_type": "application"}),
        "snapshot": (ConfigSnapshot, snapshot_payload),
        "event": (ChangeEvent, event_payload),
        "diff": (ConfigDiff, diff_payload),
        "alert": (Alert, alert_payload),
    }
    return contracts[request.param]


def test_contract_json_round_trip(contract: tuple[type[VersionedModel], dict[str, Any]]) -> None:
    model_class, payload = contract
    model = model_class.model_validate(payload)
    encoded = model.model_dump_json()

    assert json.loads(encoded)["schema_version"] == "1.0"
    assert model_class.model_validate_json(encoded) == model
    assert model_class.model_json_schema()["properties"]["schema_version"]["const"] == "1.0"


@pytest.mark.parametrize("schema_version", ["2.0", "1.1", "", 1, None])
def test_unsupported_schema_versions_are_rejected(
    contract: tuple[type[VersionedModel], dict[str, Any]], schema_version: Any
) -> None:
    model_class, payload = contract
    with pytest.raises(ValidationError):
        model_class.model_validate({**payload, "schema_version": schema_version})


def test_unknown_fields_are_rejected(contract: tuple[type[VersionedModel], dict[str, Any]]) -> None:
    model_class, payload = contract
    with pytest.raises(ValidationError, match="Extra inputs"):
        model_class.model_validate({**payload, "asset_typo": "router-001"})


@pytest.mark.parametrize("asset_id", ["", " \t\n", None, 123])
def test_invalid_asset_ids_are_rejected(
    contract: tuple[type[VersionedModel], dict[str, Any]], asset_id: Any
) -> None:
    model_class, payload = contract
    with pytest.raises(ValidationError):
        model_class.model_validate({**payload, "asset_id": asset_id})


def test_required_fields_are_enforced(
    contract: tuple[type[VersionedModel], dict[str, Any]],
) -> None:
    model_class, payload = contract
    for field_name, field in model_class.model_fields.items():
        if field.is_required():
            incomplete = {name: value for name, value in payload.items() if name != field_name}
            with pytest.raises(ValidationError, match=field_name):
                model_class.model_validate(incomplete)


@pytest.mark.parametrize("model_class", [ConfigSnapshot, ChangeEvent])
@pytest.mark.parametrize("field_name", ["event_time", "ingest_time"])
@pytest.mark.parametrize(
    "timestamp", ["invalid", "2026-09-30T03:00:00", datetime(2026, 9, 30), None]
)
def test_event_timestamps_require_a_timezone(
    model_class: type[VersionedModel],
    field_name: str,
    timestamp: Any,
    snapshot_payload: dict[str, Any],
    event_payload: dict[str, Any],
) -> None:
    payload = snapshot_payload if model_class is ConfigSnapshot else event_payload
    with pytest.raises(ValidationError):
        model_class.model_validate({**payload, field_name: timestamp})


@pytest.mark.parametrize("model_class", [Asset, Alert])
def test_created_at_requires_a_timezone(
    model_class: type[VersionedModel], alert_payload: dict[str, Any]
) -> None:
    payload = {"asset_id": "app-001", "asset_type": "application"}
    if model_class is Alert:
        payload = alert_payload
    with pytest.raises(ValidationError):
        model_class.model_validate({**payload, "created_at": "2026-09-30T03:00:00"})
    model = model_class.model_validate({**payload, "created_at": "2026-09-30T10:00:00+07:00"})
    assert model.created_at == datetime(2026, 9, 30, 3, tzinfo=UTC)


def test_malformed_json_is_rejected() -> None:
    with pytest.raises(ValidationError, match="Invalid JSON"):
        ChangeEvent.model_validate_json('{"asset_id":')
