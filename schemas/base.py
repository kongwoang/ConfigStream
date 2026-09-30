from datetime import UTC, datetime
from typing import Annotated, Literal

from pydantic import AfterValidator, AwareDatetime, BaseModel, ConfigDict, Field, StringConstraints

NonBlankString = Annotated[str, StringConstraints(strict=True, strip_whitespace=True, min_length=1)]
PositiveInteger = Annotated[int, Field(strict=True, ge=1)]
NonNegativeInteger = Annotated[int, Field(strict=True, ge=0)]


def utc_now() -> datetime:
    return datetime.now(UTC)


def as_utc(timestamp: datetime) -> datetime:
    return timestamp.astimezone(UTC)


UTCTimestamp = Annotated[AwareDatetime, AfterValidator(as_utc)]


class SchemaModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        validate_default=True,
        allow_inf_nan=False,
        hide_input_in_errors=True,
    )


class VersionedModel(SchemaModel):
    schema_version: Literal["1.0"] = "1.0"
