from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from schemas import Asset


@pytest.mark.parametrize(
    "asset_type",
    [
        "network_device",
        "nginx_server",
        "linux_server",
        "application",
        "kubernetes_resource",
        "custom",
    ],
)
def test_asset_supports_generic_types_and_metadata(asset_type: str) -> None:
    before = datetime.now(UTC)
    asset = Asset(
        asset_id="asset-001",
        asset_type=asset_type,
        metadata={"vendor": "example", "site": "hanoi", "nested": {"enabled": True, "ports": [80]}},
        tags=["production"],
    )

    assert asset.asset_type == asset_type
    assert asset.metadata["nested"] == {"enabled": True, "ports": [80]}
    assert before <= asset.created_at <= datetime.now(UTC)
    assert asset.created_at.tzinfo is UTC


def test_asset_defaults_are_not_shared() -> None:
    first = Asset(asset_id="first", asset_type="application")
    second = Asset(asset_id="second", asset_type="application")
    first.tags.append("production")
    first.metadata["site"] = "hanoi"
    assert second.tags == []
    assert second.metadata == {}


def test_labels_are_trimmed() -> None:
    asset = Asset(asset_id=" app-001 ", asset_type=" application ", tags=[" production "])
    assert asset.asset_id == "app-001"
    assert asset.asset_type == "application"
    assert asset.tags == ["production"]


@pytest.mark.parametrize("fields", [{"asset_type": " "}, {"tags": [""]}, {"vendor": "cisco"}])
def test_invalid_asset_fields_are_rejected(fields: dict) -> None:
    with pytest.raises(ValidationError):
        Asset.model_validate({"asset_id": "app-001", "asset_type": "application", **fields})


@pytest.mark.parametrize("value", [object(), datetime.now(UTC), float("nan"), float("inf")])
def test_metadata_requires_json_values(value: object) -> None:
    with pytest.raises(ValidationError):
        Asset(asset_id="app-001", asset_type="application", metadata={"invalid": value})
