from hashlib import sha256
from typing import Annotated, Self

from pydantic import (
    AnyUrl,
    Field,
    StringConstraints,
    UrlConstraints,
    field_validator,
    model_validator,
)

from schemas.base import NonBlankString, PositiveInteger, UTCTimestamp, VersionedModel

ContentURI = Annotated[AnyUrl, UrlConstraints(allowed_schemes=["s3", "https"], host_required=True)]
SHA256Digest = Annotated[str, StringConstraints(strict=True, pattern=r"^[0-9a-f]{64}$")]


class ConfigSnapshot(VersionedModel):
    snapshot_id: NonBlankString
    asset_id: NonBlankString
    version: PositiveInteger
    config_format: NonBlankString = "text"
    event_time: UTCTimestamp
    ingest_time: UTCTimestamp
    hash: SHA256Digest
    content: str | None = Field(default=None, strict=True, repr=False)
    content_uri: ContentURI | None = None

    @field_validator("content_uri")
    @classmethod
    def validate_content_uri(cls, uri: AnyUrl | None) -> AnyUrl | None:
        if uri is not None:
            if not uri.path or uri.path == "/":
                raise ValueError("content_uri must identify an object, not only a host or bucket")
            if any(
                part is not None for part in (uri.username, uri.password, uri.query, uri.fragment)
            ):
                raise ValueError(
                    "content_uri must not contain credentials, query strings, or fragments"
                )
        return uri

    @model_validator(mode="after")
    def validate_content(self) -> Self:
        if (self.content is None) == (self.content_uri is None):
            raise ValueError("provide exactly one of content or content_uri")
        if (
            self.content is not None
            and sha256(self.content.encode("utf-8")).hexdigest() != self.hash
        ):
            raise ValueError("hash must match the SHA-256 of the UTF-8 content")
        return self
