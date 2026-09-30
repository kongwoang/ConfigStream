import json
import subprocess
from importlib.metadata import PackageNotFoundError
from pathlib import Path
from threading import Event
from unittest.mock import Mock, PropertyMock

import pytest

from spark.common import runtime
from spark.common.config import KAFKA_CONNECTOR, SCALA_BINARY_VERSION, SPARK_VERSION, SparkConfig
from spark.streaming.main import main, monitor_query


def test_spark_defaults_and_pins() -> None:
    config = SparkConfig.from_env({})
    assert config.master == "local[*]"
    assert config.starting_offsets == "latest"
    assert config.bootstrap_servers == "localhost:9092"
    assert config.checkpoint_dir == Path(".cache/spark-checkpoints/config-snapshots")
    assert SPARK_VERSION == "4.0.2"
    assert SCALA_BINARY_VERSION == "2.13"
    assert KAFKA_CONNECTOR == "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2"


def test_spark_environment_loading(monkeypatch: pytest.MonkeyPatch) -> None:
    values = {
        "SPARK_MASTER": "local[2]",
        "SPARK_APP_NAME": "test-app",
        "SPARK_CHECKPOINT_DIR": ".cache/test-checkpoint",
        "SPARK_RUNTIME_DIR": ".cache/test-runtime",
        "KAFKA_BOOTSTRAP_SERVERS": "127.0.0.1:9092",
        "SPARK_STARTING_OFFSETS": "earliest",
        "SPARK_TRIGGER_SECONDS": "3",
        "SPARK_MAX_OFFSETS_PER_TRIGGER": "5",
        "SPARK_CONSOLE_ROWS": "10",
        "KAFKA_DELIVERY_TIMEOUT_MS": "irrelevant-to-spark",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    config = SparkConfig.from_env()
    assert config == SparkConfig(
        "local[2]",
        "test-app",
        "127.0.0.1:9092",
        Path(".cache/test-checkpoint"),
        Path(".cache/test-runtime"),
        "earliest",
        3,
        5,
        10,
    )


@pytest.mark.parametrize(
    "name,value",
    [
        ("SPARK_MASTER", "spark://host:7077"),
        ("SPARK_MASTER", "local[0]"),
        ("SPARK_APP_NAME", " "),
        ("KAFKA_BOOTSTRAP_SERVERS", ""),
        ("SPARK_TRIGGER_SECONDS", "0"),
        ("SPARK_TRIGGER_SECONDS", "nan"),
        ("SPARK_MAX_OFFSETS_PER_TRIGGER", "-1"),
        ("SPARK_CONSOLE_ROWS", "0"),
        ("SPARK_STARTING_OFFSETS", "invalid"),
        ("SPARK_STARTING_OFFSETS", "[]"),
        ("SPARK_STARTING_OFFSETS", '{"config.events":{"0":0}}'),
        ("SPARK_STARTING_OFFSETS", '{"config.snapshots":{}}'),
        ("SPARK_STARTING_OFFSETS", '{"config.snapshots":{"x":1}}'),
        ("SPARK_STARTING_OFFSETS", '{"config.snapshots":{"0":-1}}'),
        ("SPARK_STARTING_OFFSETS", '{"config.snapshots":{"0":true}}'),
    ],
)
def test_invalid_spark_config_is_rejected(name: str, value: str) -> None:
    with pytest.raises(ValueError):
        SparkConfig.from_env({name: value})


def test_explicit_offsets_supported() -> None:
    offsets = json.dumps({"config.snapshots": {"0": 10, "1": 20}})
    assert SparkConfig(starting_offsets=offsets).starting_offsets == offsets


@pytest.mark.parametrize("java_version", ["17.0.18", "25.0.1"])
def test_wrong_java_rejected(monkeypatch: pytest.MonkeyPatch, java_version: str) -> None:
    monkeypatch.setattr(runtime.shutil, "which", lambda name: "/mock/java")
    monkeypatch.setattr(
        runtime.subprocess,
        "run",
        Mock(
            return_value=subprocess.CompletedProcess([], 0, "", f'openjdk version "{java_version}"')
        ),
    )
    with pytest.raises(RuntimeError, match="Java 21"):
        runtime.check_runtime()


@pytest.mark.parametrize("installed", [None, "3.5.6"])
def test_missing_or_wrong_pyspark(monkeypatch: pytest.MonkeyPatch, installed: str | None) -> None:
    monkeypatch.setattr(runtime.shutil, "which", lambda name: "/mock/java")
    monkeypatch.setattr(
        runtime.subprocess,
        "run",
        Mock(return_value=subprocess.CompletedProcess([], 0, "", 'openjdk version "21.0.12"')),
    )
    monkeypatch.setattr(
        runtime,
        "version",
        Mock(side_effect=PackageNotFoundError())
        if installed is None
        else Mock(return_value=installed),
    )
    with pytest.raises(RuntimeError, match="make spark-setup"):
        runtime.check_runtime()


def test_java_home_failure_is_clear(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("JAVA_HOME", "/missing/java")
    monkeypatch.setattr(runtime.subprocess, "run", Mock(side_effect=FileNotFoundError()))
    with pytest.raises(RuntimeError, match="JAVA_HOME"):
        runtime.check_runtime()


def test_monitor_logs_only_progress_metrics(caplog: pytest.LogCaptureFixture) -> None:
    query = Mock(awaitTermination=Mock(return_value=True))
    query.lastProgress = {
        "batchId": 1,
        "numInputRows": 8,
        "inputRowsPerSecond": 2.0,
        "processedRowsPerSecond": 4.0,
        "unused": "not-for-logs",
    }
    with caplog.at_level("INFO"):
        monitor_query(query, available_now=True)
    assert "numInputRows=8" in caplog.text
    assert "not-for-logs" not in caplog.text


def test_unexpected_termination_fails() -> None:
    query = Mock(awaitTermination=Mock(return_value=True), lastProgress=None)
    with pytest.raises(RuntimeError, match="unexpectedly"):
        monitor_query(query, available_now=False)


def test_idle_progress_does_not_hide_next_data_batch(caplog: pytest.LogCaptureFixture) -> None:
    query = Mock(awaitTermination=Mock(side_effect=[False, True]))
    idle = {
        "batchId": 1,
        "numInputRows": 0,
        "inputRowsPerSecond": 0.0,
        "processedRowsPerSecond": 0.0,
    }
    type(query).lastProgress = PropertyMock(side_effect=[idle, {**idle, "numInputRows": 8}])
    with caplog.at_level("INFO"):
        monitor_query(query, available_now=True)
    assert "numInputRows=8" in caplog.text


def test_query_failure_is_not_swallowed() -> None:
    query = Mock(awaitTermination=Mock(side_effect=RuntimeError("Kafka unavailable")))
    with pytest.raises(RuntimeError, match="Kafka unavailable"):
        monitor_query(query, available_now=True)


def test_shutdown_flag_does_not_call_spark_inside_signal_handler() -> None:
    stopped = Event()
    stopped.set()
    query = Mock()
    with pytest.raises(KeyboardInterrupt):
        monitor_query(query, available_now=False, stop_requested=stopped)
    query.awaitTermination.assert_not_called()


def test_cli_startup_failure_is_nonzero(monkeypatch: pytest.MonkeyPatch, caplog) -> None:
    monkeypatch.setattr(
        "spark.streaming.main.create_session", Mock(side_effect=RuntimeError("connector missing"))
    )
    assert main(["--available-now"]) == 1
    assert "Spark streaming failed" in caplog.text


def test_normal_interrupt_exits_without_traceback(monkeypatch: pytest.MonkeyPatch, caplog) -> None:
    monkeypatch.setattr(
        "spark.streaming.main.create_session", Mock(side_effect=KeyboardInterrupt())
    )
    assert main([]) == 0
    assert "Traceback" not in caplog.text
