from typing import Literal, Self

from pydantic import Field, model_validator

from schemas.base import NonBlankString, NonNegativeInteger, SchemaModel, VersionedModel


class DiffLine(SchemaModel):
    operation: Literal["added", "removed"]
    line: str = Field(strict=True)


class ConfigDiff(VersionedModel):
    diff_id: NonBlankString
    asset_id: NonBlankString
    old_snapshot_id: NonBlankString | None = None
    new_snapshot_id: NonBlankString
    added: NonNegativeInteger
    removed: NonNegativeInteger
    changed: NonNegativeInteger = 0
    details: list[DiffLine] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_summary(self) -> Self:
        if self.old_snapshot_id == self.new_snapshot_id:
            raise ValueError("old_snapshot_id and new_snapshot_id must differ")
        if self.added != sum(detail.operation == "added" for detail in self.details):
            raise ValueError("added must match the number of added details")
        if self.removed != sum(detail.operation == "removed" for detail in self.details):
            raise ValueError("removed must match the number of removed details")
        if self.changed > min(self.added, self.removed):
            raise ValueError("changed cannot exceed paired added and removed lines")
        if self.old_snapshot_id is None and self.removed:
            raise ValueError("a baseline diff cannot remove lines")
        return self
