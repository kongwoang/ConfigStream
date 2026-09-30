import json
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from messaging.config import KafkaConfig

SPARK_VERSION = "4.0.2"
SCALA_BINARY_VERSION = "2.13"
KAFKA_CONNECTOR = f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_BINARY_VERSION}:{SPARK_VERSION}"
SNAPSHOTS_TOPIC = "config.snapshots"


def validate_offsets(value: str) -> None:
    if value in {"earliest", "latest"}:
        return
    try:
        offsets = json.loads(value)
        if not isinstance(offsets, dict) or set(offsets) != {SNAPSHOTS_TOPIC}:
            raise ValueError
        partitions = offsets[SNAPSHOTS_TOPIC]
        if not isinstance(partitions, dict) or not partitions:
            raise ValueError
        for partition, offset in partitions.items():
            if not partition.isdigit() or type(offset) is not int or offset < 0:
                raise ValueError
    except (ValueError, TypeError) as error:
        raise ValueError(
            "SPARK_STARTING_OFFSETS must be earliest, latest, or explicit nonnegative "
            "config.snapshots partition offsets"
        ) from error


@dataclass(frozen=True, slots=True)
class SparkConfig:
    master: str = "local[*]"
    app_name: str = "ConfigStreamStreaming"
    bootstrap_servers: str = "localhost:9092"
    checkpoint_dir: Path = Path(".cache/spark-checkpoints/config-snapshots")
    runtime_dir: Path = Path(".cache/spark")
    starting_offsets: str = "latest"
    trigger_seconds: int = 2
    max_offsets_per_trigger: int = 10_000
    console_rows: int = 20

    def __post_init__(self) -> None:
        if not re.fullmatch(r"local(?:\[(?:\*|[1-9]\d*)\])?", self.master):
            raise ValueError("SPARK_MASTER must use local mode, for example local[*] or local[2]")
        if not self.app_name.strip() or not self.bootstrap_servers.strip():
            raise ValueError("Spark app name and Kafka bootstrap servers must not be empty")
        for value in (self.trigger_seconds, self.max_offsets_per_trigger, self.console_rows):
            if type(value) is not int or value <= 0:
                raise ValueError(
                    "Spark trigger seconds, offset limit, and console rows must be positive"
                )
        validate_offsets(self.starting_offsets)

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "SparkConfig":
        values = os.environ if environ is None else environ
        return cls(
            master=values.get("SPARK_MASTER", "local[*]"),
            app_name=values.get("SPARK_APP_NAME", "ConfigStreamStreaming"),
            bootstrap_servers=values.get(
                "KAFKA_BOOTSTRAP_SERVERS", KafkaConfig().bootstrap_servers
            ),
            checkpoint_dir=Path(
                values.get("SPARK_CHECKPOINT_DIR", ".cache/spark-checkpoints/config-snapshots")
            ),
            runtime_dir=Path(values.get("SPARK_RUNTIME_DIR", ".cache/spark")),
            starting_offsets=values.get("SPARK_STARTING_OFFSETS", "latest"),
            trigger_seconds=int(values.get("SPARK_TRIGGER_SECONDS", "2")),
            max_offsets_per_trigger=int(values.get("SPARK_MAX_OFFSETS_PER_TRIGGER", "10000")),
            console_rows=int(values.get("SPARK_CONSOLE_ROWS", "20")),
        )
