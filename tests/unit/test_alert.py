from datetime import UTC, datetime
from typing import Any

import pytest
from pydantic import ValidationError

from schemas import Alert, Severity


@pytest.mark.parametrize("severity", ["info", "low", "medium", "high", "critical"])
def test_alert_supports_defined_severities(alert_payload: dict[str, Any], severity: str) -> None:
    before = datetime.now(UTC)
    alert = Alert.model_validate({**alert_payload, "severity": severity})
    assert alert.severity is Severity(severity)
    assert alert.model_dump(mode="json")["severity"] == severity
    assert before <= alert.created_at <= datetime.now(UTC)
    assert alert.created_at.tzinfo is UTC


@pytest.mark.parametrize("severity", ["urgent", "HIGH", "", 5, None])
def test_invalid_severity_is_rejected(alert_payload: dict[str, Any], severity: Any) -> None:
    with pytest.raises(ValidationError):
        Alert.model_validate({**alert_payload, "severity": severity})


@pytest.mark.parametrize("field_name", ["alert_id", "snapshot_id", "rule_id", "message"])
def test_blank_alert_fields_are_rejected(alert_payload: dict[str, Any], field_name: str) -> None:
    with pytest.raises(ValidationError):
        Alert.model_validate({**alert_payload, field_name: " "})
