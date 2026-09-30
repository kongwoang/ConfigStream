import json
import os
import socket
import subprocess
import sys
from collections import Counter
from dataclasses import replace

import pytest
from confluent_kafka import TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec
from pyspark.sql import DataFrame

from generator.engine import WorkloadGenerator
from generator.main import publish_workload
from messaging.config import KafkaConfig
from messaging.kafka_producer import KafkaProducer
from scripts.kafka_invalid_demo import publish_invalid_records
from spark.common.config import SNAPSHOTS_TOPIC, SparkConfig
from spark.common.runtime import create_session
from spark.streaming.kafka_source import read_snapshots
from spark.streaming.parser import parse_records
from spark.streaming.validation import classify_records, console_rows

pytestmark = pytest.mark.spark_integration


def test_kafka_snapshots_invalid_records_and_checkpoint_resume(tmp_path) -> None:
    kafka_config = KafkaConfig.from_env()
    admin = AdminClient(kafka_config.client_options())
    metadata = admin.list_topics(timeout=10).topics[SNAPSHOTS_TOPIC]
    futures = admin.list_offsets(
        {
            TopicPartition(SNAPSHOTS_TOPIC, partition): OffsetSpec.latest()
            for partition in metadata.partitions
        },
        request_timeout=10,
    )
    offsets = {
        str(partition.partition): future.result(timeout=10).offset
        for partition, future in futures.items()
    }
    config = replace(
        SparkConfig.from_env(),
        master="local[2]",
        checkpoint_dir=tmp_path / "checkpoint",
        starting_offsets=json.dumps({SNAPSHOTS_TOPIC: offsets}),
        max_offsets_per_trigger=5,
    )
    session = create_session(config)
    try:
        generator = WorkloadGenerator(assets=3, seed=42)
        publish_workload(generator, events=8, paced=False)
        assert publish_invalid_records(kafka_config) == 4

        def run_bounded(current_config: SparkConfig) -> tuple[list, list]:
            received = []

            def capture(batch: DataFrame, batch_id: int) -> None:
                received.extend(console_rows(batch).collect())

            classified = classify_records(parse_records(read_snapshots(session, current_config)))
            assert classified.isStreaming
            query = (
                classified.writeStream.foreachBatch(capture)
                .option("checkpointLocation", str(current_config.checkpoint_dir))
                .trigger(availableNow=True)
                .start()
            )
            try:
                assert query.awaitTermination(90), "availableNow did not finish"
                assert query.exception() is None
                progress = query.recentProgress
                return received, progress
            finally:
                query.stop()

        received, progress = run_bounded(config)
        assert Counter(row.record_status for row in received) == {"valid": 8, "invalid": 4}
        assert Counter(
            row.validation_error for row in received if row.record_status == "invalid"
        ) == {
            "malformed_json": 1,
            "wrong_record_type": 1,
            "missing_asset_id": 1,
            "kafka_key_mismatch": 1,
        }
        assert sum(batch["numInputRows"] for batch in progress) == 12
        assert "SENSITIVE_TEST_MARKER" not in str(received)
        for row in received:
            assert row.kafka_topic == SNAPSHOTS_TOPIC
            assert row.kafka_offset >= offsets[str(row.kafka_partition)]
            if row.record_status == "valid":
                assert row.kafka_key == row.asset_id
                assert row.event_time is not None
                assert row.version > 0
        assert (config.checkpoint_dir / "commits" / "0").exists()
        resumed_config = replace(config, starting_offsets="earliest")
        resumed, _ = run_bounded(resumed_config)
        assert resumed == []
        snapshot = generator.next_change()[1]
        with KafkaProducer(kafka_config) as producer:
            producer.publish(snapshot)
        newly_received, _ = run_bounded(resumed_config)
        assert len(newly_received) == 1
        assert newly_received[0].snapshot_id == snapshot.snapshot_id
        assert newly_received[0].record_status == "valid"
    finally:
        session.stop()


def test_unavailable_broker_fails_cli(tmp_path) -> None:
    with socket.socket() as unavailable:
        unavailable.bind(("127.0.0.1", 0))
        port = unavailable.getsockname()[1]
        result = subprocess.run(
            [sys.executable, "-m", "spark.streaming.main", "--available-now"],
            env={
                **os.environ,
                "KAFKA_BOOTSTRAP_SERVERS": f"127.0.0.1:{port}",
                "SPARK_MASTER": "local[2]",
                "SPARK_CHECKPOINT_DIR": str(tmp_path / "unavailable-checkpoint"),
                "SPARK_STARTING_OFFSETS": "latest",
            },
            capture_output=True,
            text=True,
            timeout=75,
        )
    assert result.returncode == 1, result.stderr
    assert "Spark streaming failed" in result.stderr
    assert "TimeoutException" in result.stderr
    assert "numInputRows=" not in result.stderr
