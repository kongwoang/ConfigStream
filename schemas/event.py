from pydantic import Field, JsonValue

from schemas.base import NonBlankString, UTCTimestamp, VersionedModel


class ChangeEvent(VersionedModel):
    event_id: NonBlankString
    asset_id: NonBlankString
    event_type: NonBlankString = "config_changed"
    source: NonBlankString
    event_time: UTCTimestamp
    ingest_time: UTCTimestamp
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
