from enum import StrEnum

from pydantic import Field

from schemas.base import NonBlankString, UTCTimestamp, VersionedModel, utc_now


class Severity(StrEnum):
    INFO = "info"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class Alert(VersionedModel):
    alert_id: NonBlankString
    asset_id: NonBlankString
    snapshot_id: NonBlankString
    rule_id: NonBlankString
    severity: Severity
    message: NonBlankString
    created_at: UTCTimestamp = Field(default_factory=utc_now)
