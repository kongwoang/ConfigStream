from dataclasses import dataclass, field
from hashlib import sha256

from schemas import ConfigDiff


@dataclass(frozen=True, slots=True)
class NormalizedConfig:
    """Internal text representation; construct through normalize_snapshot."""

    asset_id: str
    snapshot_id: str
    version: int
    normalized_content: str = field(repr=False)
    normalized_hash: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "normalized_hash",
            sha256(self.normalized_content.encode("utf-8")).hexdigest(),
        )


@dataclass(frozen=True, slots=True)
class DiffResult:
    """Pair diagnostics without extending the public ConfigDiff contract."""

    diff: ConfigDiff = field(repr=False)
    version_gap: int
    old_normalized_hash: str
    new_normalized_hash: str
