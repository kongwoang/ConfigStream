from dataclasses import dataclass
from datetime import datetime

from schemas import Asset


@dataclass(slots=True)
class AssetState:
    asset: Asset
    current_config: dict[str, str]
    version: int = 0
    last_snapshot_id: str | None = None
    last_event_time: datetime | None = None
