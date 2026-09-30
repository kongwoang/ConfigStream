from pydantic import Field, JsonValue

from schemas.base import NonBlankString, UTCTimestamp, VersionedModel, utc_now


class Asset(VersionedModel):
    asset_id: NonBlankString
    asset_type: NonBlankString
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    tags: list[NonBlankString] = Field(default_factory=list)
    created_at: UTCTimestamp = Field(default_factory=utc_now)
