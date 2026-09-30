import json
from collections.abc import Iterator
from datetime import UTC, datetime

import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as functions
from pyspark.sql.types import TimestampType

from generator.engine import WorkloadGenerator
from spark.common.config import SparkConfig
from spark.common.runtime import create_session
from spark.common.schemas import KAFKA_COLUMNS, SNAPSHOT_COLUMNS, SNAPSHOT_SCHEMA
from spark.streaming.kafka_source import read_snapshots
from spark.streaming.parser import parse_records
from spark.streaming.validation import classify_records, console_rows, valid_snapshots

CASES = [
    ("valid", {}, None),
    ("empty_content", {"content": ""}, None),
    ("uri", {"content": None, "content_uri": "s3://bucket/config.txt"}, None),
    ("timezone_offset", {"event_time": "2026-09-30T07:00:00+07:00"}, None),
    ("schema_version", {"schema_version": "2.0"}, "unsupported_schema_version"),
    ("no_schema_version", {"schema_version": None}, "unsupported_schema_version"),
    ("snapshot_id", {"snapshot_id": " "}, "missing_snapshot_id"),
    ("asset_id", {"asset_id": None}, "missing_asset_id"),
    ("blank_asset_id", {"asset_id": " \t"}, "missing_asset_id"),
    ("zero_version", {"version": 0}, "invalid_version"),
    ("negative_version", {"version": -1}, "invalid_version"),
    ("no_version", {"version": None}, "invalid_version"),
    ("string_version", {"version": "1"}, "malformed_json"),
    ("config_format", {"config_format": ""}, "invalid_config_format"),
    ("bad_event_time", {"event_time": "invalid"}, "invalid_event_time"),
    ("impossible_date", {"event_time": "2026-02-30T00:00:00Z"}, "invalid_event_time"),
    ("naive_time", {"event_time": "2026-09-30T00:00:00"}, "invalid_event_time"),
    ("bad_ingest_time", {"ingest_time": None}, "invalid_ingest_time"),
    ("empty_hash", {"hash": ""}, "invalid_hash"),
    ("bad_hash", {"hash": "not-a-hash"}, "invalid_hash"),
    ("both_locations", {"content_uri": "s3://bucket/config.txt"}, "invalid_content_location"),
    ("neither_location", {"content": None}, "invalid_content_location"),
    ("blank_uri", {"content": None, "content_uri": " "}, "invalid_content_location"),
]


@pytest.fixture(scope="module")
def session(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SparkSession]:
    config = SparkConfig(master="local[2]", runtime_dir=tmp_path_factory.mktemp("spark-runtime"))
    spark_session = create_session(config, kafka=False)
    yield spark_session
    spark_session.stop()


@pytest.fixture(scope="module")
def classified(session: SparkSession) -> DataFrame:
    snapshot = WorkloadGenerator(assets=1).next_change()[1]
    payload = snapshot.model_dump(mode="json")
    rows = []
    for name, changes, _ in CASES:
        envelope = {"record_type": "config_snapshot", "payload": {**payload, **changes}}
        rows.append((name, snapshot.asset_id, json.dumps(envelope)))
    rows.extend(
        [
            ("invalid_json", snapshot.asset_id, '{"secret":"SENSITIVE_TEST_MARKER"'),
            ("null_json", snapshot.asset_id, "null"),
            ("array_json", snapshot.asset_id, "[]"),
            ("no_payload", snapshot.asset_id, '{"record_type":"config_snapshot"}'),
            (
                "wrong_record_type",
                snapshot.asset_id,
                json.dumps({"record_type": "asset", "payload": payload}),
            ),
            (
                "wrong_key",
                "another-asset",
                json.dumps({"record_type": "config_snapshot", "payload": payload}),
            ),
            (
                "missing_key",
                None,
                json.dumps({"record_type": "config_snapshot", "payload": payload}),
            ),
            ("tombstone", snapshot.asset_id, None),
        ]
    )
    frame = session.createDataFrame(
        [
            (
                key.encode() if key else None,
                value.encode() if value else None,
                "config.snapshots",
                2,
                index,
                datetime(2026, 9, 30, tzinfo=UTC),
                name,
            )
            for index, (name, key, value) in enumerate(rows)
        ],
        "key binary, value binary, topic string, partition int, offset long, "
        "timestamp timestamp, name string",
    )
    parsed = classify_records(parse_records(frame))
    return parsed.join(
        frame.select(functions.col("offset").alias("kafka_offset"), "name"), "kafka_offset"
    ).cache()


@pytest.fixture(scope="module")
def results(classified: DataFrame) -> dict:
    return {row.name: row for row in classified.collect()}


@pytest.mark.parametrize("name,changes,error", CASES)
def test_payload_validation(results: dict, name: str, changes: dict, error: str | None) -> None:
    assert results[name].validation_error == error
    assert results[name].record_status == ("invalid" if error else "valid")


@pytest.mark.parametrize(
    "name,error",
    [
        ("invalid_json", "malformed_json"),
        ("null_json", "malformed_json"),
        ("array_json", "malformed_json"),
        ("no_payload", "missing_payload"),
        ("wrong_record_type", "wrong_record_type"),
        ("wrong_key", "kafka_key_mismatch"),
        ("missing_key", "kafka_key_mismatch"),
        ("tombstone", "malformed_json"),
    ],
)
def test_envelope_and_key_validation(results: dict, name: str, error: str) -> None:
    assert results[name].validation_error == error


def test_utc_timestamps_and_metadata(
    session: SparkSession, classified: DataFrame, results: dict
) -> None:
    assert session.conf.get("spark.sql.session.timeZone") == "UTC"
    assert isinstance(classified.schema["event_time"].dataType, TimestampType)
    assert isinstance(classified.schema["ingest_time"].dataType, TimestampType)
    for name in ("valid", "timezone_offset"):
        epoch = (
            classified.filter(functions.col("name") == name)
            .select(functions.unix_micros("event_time"))
            .first()[0]
        )
        assert epoch == int(datetime(2026, 9, 30, tzinfo=UTC).timestamp() * 1_000_000)
    record = results["valid"]
    assert record.kafka_key == record.asset_id
    assert record.kafka_topic == "config.snapshots"
    assert record.kafka_partition == 2
    assert record.kafka_offset == 0
    assert record.kafka_timestamp is not None


def test_valid_output_retains_content_without_diagnostics(classified: DataFrame) -> None:
    valid = valid_snapshots(classified)
    assert valid.columns == SNAPSHOT_COLUMNS + KAFKA_COLUMNS
    assert valid.count() == 4
    assert "logging enabled" in valid.filter(functions.col("kafka_offset") == 0).first().content
    assert "PythonUDF" not in valid._jdf.queryExecution().optimizedPlan().toString()


def test_console_does_not_disclose_config_or_corrupt_payload(classified: DataFrame) -> None:
    safe = console_rows(classified)
    assert not {"content", "content_uri", "_json", "_corrupt_record"} & set(safe.columns)
    output = safe.toJSON().collect()
    assert "SENSITIVE_TEST_MARKER" not in "".join(output)
    assert "logging enabled" not in "".join(output)
    for row in safe.filter(functions.col("record_status") == "invalid").collect():
        assert row.validation_error
        assert row.asset_id is None


def test_sql_schema_tracks_existing_snapshot_fields() -> None:
    from schemas import ConfigSnapshot

    assert SNAPSHOT_SCHEMA.fieldNames() == list(ConfigSnapshot.model_fields)


def test_missing_connector_is_a_visible_failure(session: SparkSession) -> None:
    with pytest.raises(AnalysisException, match="kafka"):
        read_snapshots(session, SparkConfig(master="local[2]"))
