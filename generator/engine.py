import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from random import Random
from uuid import NAMESPACE_URL, uuid5

from generator.mutations import mutate_config
from generator.state import AssetState
from generator.templates import DEFAULT_ASSET_MIX, TEMPLATES, initial_config, render_config
from schemas import Asset, ChangeEvent, ConfigSnapshot

DEFAULT_START_TIME = datetime(2026, 9, 30, tzinfo=UTC)
INGEST_DELAY = timedelta(milliseconds=5)
MAX_RATE = 1_000_000


class WorkloadGenerator:
    def __init__(
        self,
        assets: int = 100,
        seed: int = 42,
        rate: int = 100,
        start_time: datetime = DEFAULT_START_TIME,
        asset_mix: tuple[str, ...] = DEFAULT_ASSET_MIX,
    ) -> None:
        if type(assets) is not int or assets < 1:
            raise ValueError("assets must be a positive integer")
        if type(rate) is not int or not 1 <= rate <= MAX_RATE:
            raise ValueError(f"rate must be an integer between 1 and {MAX_RATE}")
        if type(seed) is not int:
            raise ValueError("seed must be an integer")
        if start_time.tzinfo is None or start_time.utcoffset() is None:
            raise ValueError("start_time must include a timezone")
        if not asset_mix or any(asset_type not in TEMPLATES for asset_type in asset_mix):
            raise ValueError(f"asset_mix must contain supported types: {', '.join(TEMPLATES)}")

        self.rate = rate
        self.start_time = start_time.astimezone(UTC)
        self.rng = Random(seed)
        self.sequence = 0
        self.namespace = uuid5(
            NAMESPACE_URL,
            json.dumps(
                {
                    "generator_version": 1,
                    "assets": assets,
                    "seed": seed,
                    "rate": rate,
                    "start_time": self.start_time.isoformat(),
                    "asset_mix": asset_mix,
                },
                sort_keys=True,
            ),
        )
        self.states: list[AssetState] = []
        for index in range(assets):
            asset_type = asset_mix[index % len(asset_mix)]
            asset_id = f"{TEMPLATES[asset_type].prefix}-{index + 1:06d}"
            asset = Asset(
                asset_id=asset_id,
                asset_type=asset_type,
                metadata={"source": "synthetic"},
                tags=["synthetic"],
                created_at=self.start_time,
            )
            self.states.append(AssetState(asset, initial_config(asset_type, asset_id)))

    def next_change(self) -> tuple[ChangeEvent, ConfigSnapshot]:
        state = self.rng.choice(self.states)
        mutation = mutate_config(state.asset.asset_type, state.current_config, self.rng)
        version = state.version + 1
        event_time = self.start_time + timedelta(
            microseconds=self.sequence * 1_000_000 // self.rate
        )
        ingest_time = event_time + INGEST_DELAY
        identity = f"{state.asset.asset_id}:{version}:{self.sequence}"
        snapshot_id = f"snap-{uuid5(self.namespace, 'snapshot:' + identity)}"
        content = render_config(mutation.config)
        snapshot = ConfigSnapshot(
            snapshot_id=snapshot_id,
            asset_id=state.asset.asset_id,
            version=version,
            config_format="text",
            event_time=event_time,
            ingest_time=ingest_time,
            hash=sha256(content.encode("utf-8")).hexdigest(),
            content=content,
        )
        event = ChangeEvent(
            event_id=f"evt-{uuid5(self.namespace, 'event:' + identity)}",
            asset_id=state.asset.asset_id,
            event_type="config_changed",
            source="synthetic",
            event_time=event_time,
            ingest_time=ingest_time,
            metadata={
                "mutation": mutation.name,
                "setting": mutation.setting,
                "old_value": mutation.old_value,
                "new_value": mutation.new_value,
                "snapshot_id": snapshot_id,
                "previous_snapshot_id": state.last_snapshot_id,
                "asset_type": state.asset.asset_type,
                "sequence": self.sequence,
            },
        )
        state.current_config = mutation.config
        state.version = version
        state.last_snapshot_id = snapshot_id
        state.last_event_time = event_time
        self.sequence += 1
        return event, snapshot

    def generate(self, events: int) -> Iterator[tuple[ChangeEvent, ConfigSnapshot]]:
        if type(events) is not int or events < 0:
            raise ValueError("events must be a nonnegative integer")
        if events:
            last_sequence = self.sequence + events - 1
            latest_event_time = datetime.max.replace(tzinfo=UTC) - INGEST_DELAY
            maximum_offset = (latest_event_time - self.start_time) // timedelta(microseconds=1)
            if last_sequence * 1_000_000 // self.rate > maximum_offset:
                raise ValueError("workload exceeds the supported timestamp range")
        for _ in range(events):
            yield self.next_change()
