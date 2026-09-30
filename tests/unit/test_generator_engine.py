import random
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Any

import pytest

from generator.engine import DEFAULT_START_TIME, INGEST_DELAY, WorkloadGenerator
from generator.serialization import serialize_change
from generator.templates import DEFAULT_ASSET_MIX, TEMPLATES, initial_config, render_config
from schemas import Asset, ChangeEvent, ConfigSnapshot


def test_stateful_history_invariants() -> None:
    generator = WorkloadGenerator(assets=12, seed=42, rate=137)
    histories = {
        state.asset.asset_id: (0, None, state.current_config.copy(), None)
        for state in generator.states
    }
    states = {state.asset.asset_id: state for state in generator.states}
    record_ids: set[str] = set()

    for sequence, (event, snapshot) in enumerate(generator.generate(500)):
        previous_version, previous_id, previous_config, previous_time = histories[event.asset_id]
        state = states[event.asset_id]
        assert snapshot.asset_id == event.asset_id == state.asset.asset_id
        assert snapshot.version == previous_version + 1 == state.version
        assert event.metadata["snapshot_id"] == snapshot.snapshot_id == state.last_snapshot_id
        assert event.metadata["previous_snapshot_id"] == previous_id
        assert event.metadata["asset_type"] == state.asset.asset_type
        assert event.metadata["sequence"] == sequence
        assert event.event_type == "config_changed"
        assert event.source == "synthetic"
        assert snapshot.config_format == "text"
        assert snapshot.content_uri is None
        assert snapshot.content != render_config(previous_config)
        assert snapshot.content == render_config(state.current_config)
        assert snapshot.hash == sha256(snapshot.content.encode("utf-8")).hexdigest()
        assert event.event_time == snapshot.event_time == state.last_event_time
        assert event.ingest_time == snapshot.ingest_time == event.event_time + INGEST_DELAY
        assert event.event_time == DEFAULT_START_TIME + timedelta(
            microseconds=sequence * 1_000_000 // 137
        )
        assert event.event_time.tzinfo is UTC
        assert previous_time is None or event.event_time > previous_time
        assert snapshot.snapshot_id not in record_ids
        assert event.event_id not in record_ids
        record_ids.update((snapshot.snapshot_id, event.event_id))
        assert ChangeEvent.model_validate_json(event.model_dump_json()) == event
        assert ConfigSnapshot.model_validate_json(snapshot.model_dump_json()) == snapshot

        setting = event.metadata["setting"]
        assert previous_config.get(setting) == event.metadata["old_value"]
        expected = previous_config.copy()
        if event.metadata["new_value"] is None:
            del expected[setting]
        else:
            expected[setting] = event.metadata["new_value"]
        assert expected == state.current_config
        histories[event.asset_id] = (
            state.version,
            state.last_snapshot_id,
            expected,
            state.last_event_time,
        )
        for asset_id, other in states.items():
            version, snapshot_id, config, event_time = histories[asset_id]
            assert (
                other.version,
                other.last_snapshot_id,
                other.current_config,
                other.last_event_time,
            ) == (version, snapshot_id, config, event_time)

    assert all(state.version > 0 for state in states.values())
    assert sum(state.version for state in states.values()) == 500


def test_same_seed_produces_byte_identical_output() -> None:
    first = WorkloadGenerator(assets=10, seed=42)
    second = WorkloadGenerator(assets=10, seed=42)
    assert [serialize_change(*change) for change in first.generate(100)] == [
        serialize_change(*change) for change in second.generate(100)
    ]


def test_different_seeds_change_selection_and_mutations() -> None:
    sequences = []
    for seed in (42, 43):
        generator = WorkloadGenerator(assets=10, seed=seed)
        sequences.append(
            [
                (event.asset_id, event.metadata["mutation"], snapshot.content)
                for event, snapshot in generator.generate(100)
            ]
        )
    assert sequences[0] != sequences[1]


def test_generator_continues_across_calls_and_preserves_prefix() -> None:
    whole = WorkloadGenerator(assets=4)
    chunked = WorkloadGenerator(assets=4)
    expected = list(whole.generate(20))
    assert list(chunked.generate(7)) + list(chunked.generate(13)) == expected


@pytest.mark.parametrize("asset_type", DEFAULT_ASSET_MIX)
def test_single_asset_type_works_independently(asset_type: str) -> None:
    generator = WorkloadGenerator(assets=2, asset_mix=(asset_type,))
    for state in generator.states:
        assert state.asset.asset_type == asset_type
        assert state.asset.created_at == DEFAULT_START_TIME
        assert state.version == 0
        assert state.last_snapshot_id is None
        assert state.last_event_time is None
        assert state.current_config == initial_config(asset_type, state.asset.asset_id)
        assert Asset.model_validate_json(state.asset.model_dump_json()) == state.asset
    assert len(list(generator.generate(50))) == 50


def test_asset_mix_is_cyclic_and_supports_repeated_types() -> None:
    mix = ("generic_service", "nginx_server", "generic_service")
    generator = WorkloadGenerator(assets=7, asset_mix=mix)
    assert [state.asset.asset_type for state in generator.states] == [
        mix[index % 3] for index in range(7)
    ]
    assert len({state.asset.asset_id for state in generator.states}) == 7


def test_generator_does_not_change_global_random_state() -> None:
    before = random.getstate()
    list(WorkloadGenerator().generate(10))
    assert random.getstate() == before


def test_timezone_equivalent_start_times_have_identical_output() -> None:
    offset = datetime.fromisoformat("2026-09-30T07:00:00+07:00")
    first = WorkloadGenerator(start_time=offset)
    second = WorkloadGenerator(start_time=DEFAULT_START_TIME)
    assert list(first.generate(10)) == list(second.generate(10))


@pytest.mark.parametrize("rate", [1, 3, 1_000_000])
def test_simulated_time_uses_integer_microseconds(rate: int) -> None:
    generator = WorkloadGenerator(assets=1, rate=rate)
    times = [event.event_time for event, _ in generator.generate(10)]
    assert times == [
        DEFAULT_START_TIME + timedelta(microseconds=index * 1_000_000 // rate)
        for index in range(10)
    ]
    assert all(previous < current for previous, current in zip(times, times[1:], strict=False))


@pytest.mark.parametrize(
    "settings",
    [
        {"assets": 0},
        {"assets": -1},
        {"assets": True},
        {"assets": 2.5},
        {"rate": 0},
        {"rate": -1},
        {"rate": 1_000_001},
        {"rate": 1.5},
        {"rate": True},
        {"seed": "42"},
        {"seed": True},
        {"start_time": datetime(2026, 9, 30)},
        {"asset_mix": ()},
        {"asset_mix": ("unknown",)},
    ],
)
def test_invalid_engine_settings_are_rejected(settings: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        WorkloadGenerator(**settings)


@pytest.mark.parametrize("events", [-1, True, 1.5])
def test_invalid_event_count_is_rejected(events: Any) -> None:
    generator = WorkloadGenerator()
    with pytest.raises(ValueError):
        list(generator.generate(events))
    assert generator.sequence == 0


def test_zero_events_does_not_change_state() -> None:
    generator = WorkloadGenerator()
    assert list(generator.generate(0)) == []
    assert generator.sequence == 0
    assert all(state.version == 0 for state in generator.states)


def test_timestamp_overflow_is_rejected_before_advancing_state() -> None:
    generator = WorkloadGenerator(start_time=datetime.max.replace(tzinfo=UTC) - INGEST_DELAY)
    with pytest.raises(ValueError, match="timestamp range"):
        list(generator.generate(2))
    assert generator.sequence == 0
    event, snapshot = next(generator.generate(1))
    assert event.ingest_time == snapshot.ingest_time == datetime.max.replace(tzinfo=UTC)


def test_templates_are_not_modified_by_generator() -> None:
    original = {asset_type: template.base.copy() for asset_type, template in TEMPLATES.items()}
    list(WorkloadGenerator(assets=3).generate(200))
    assert {asset_type: template.base for asset_type, template in TEMPLATES.items()} == original
