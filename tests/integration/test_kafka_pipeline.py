import json
import os
import socket
import subprocess
import sys
from collections import Counter
from time import monotonic
from uuid import uuid4

import pytest

from generator.engine import WorkloadGenerator
from messaging.config import KafkaConfig
from messaging.serialization import serialize_record
from messaging.topics import TOPICS, topic_for
from schemas import Asset, ChangeEvent, ConfigSnapshot

pytestmark = pytest.mark.integration


def test_required_topics_and_configuration() -> None:
    from confluent_kafka.admin import AdminClient, ConfigResource, ResourceType

    admin = AdminClient(KafkaConfig.from_env().client_options())
    metadata = admin.list_topics(timeout=10)
    assert len(metadata.brokers) == 1
    for topic in TOPICS:
        actual = metadata.topics[topic.name]
        assert not actual.error
        assert len(actual.partitions) == topic.partitions
        assert all(len(partition.replicas) == 1 for partition in actual.partitions.values())
        resource = ConfigResource(ResourceType.TOPIC, topic.name)
        configs = admin.describe_configs([resource])[resource].result(timeout=10)
        for name, value in topic.configs().items():
            assert configs[name].value == value


def test_cli_publishes_expected_keyed_schema_valid_records() -> None:
    from confluent_kafka import Consumer, TopicPartition

    config = KafkaConfig.from_env()
    seed = uuid4().int % 2**32
    expected_generator = WorkloadGenerator(assets=3, seed=seed)
    expected = [state.asset for state in expected_generator.states]
    for event, snapshot in expected_generator.generate(8):
        expected.extend((event, snapshot))
    expected_records = Counter(
        (topic_for(record), record.asset_id.encode(), serialize_record(record).encode())
        for record in expected
    )
    consumer = Consumer(
        {
            **config.client_options(),
            "group.id": f"configstream-test-{uuid4()}",
            "enable.auto.commit": False,
            "auto.offset.reset": "error",
            "allow.auto.create.topics": False,
        }
    )
    try:
        partitions = []
        metadata = consumer.list_topics(timeout=10)
        for topic in TOPICS[:3]:
            for partition in metadata.topics[topic.name].partitions:
                topic_partition = TopicPartition(topic.name, partition)
                _, high = consumer.get_watermark_offsets(topic_partition, timeout=10)
                partitions.append(TopicPartition(topic.name, partition, high))
        consumer.assign(partitions)
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "generator.main",
                "--assets",
                "3",
                "--events",
                "8",
                "--rate",
                "100",
                "--seed",
                str(seed),
                "--no-sleep",
                "--sink",
                "kafka",
            ],
            capture_output=True,
            text=True,
            timeout=40,
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout == ""
        assert "confirmed 19 records" in result.stderr
        received = []
        observed_partitions = {}
        deadline = monotonic() + 20
        models = {"asset": Asset, "change_event": ChangeEvent, "config_snapshot": ConfigSnapshot}
        while len(received) < 19 and monotonic() < deadline:
            message = consumer.poll(0.5)
            if message is None:
                continue
            assert not message.error(), message.error()
            envelope = json.loads(message.value())
            record = models[envelope["record_type"]].model_validate(envelope["payload"])
            assert message.key() == record.asset_id.encode("utf-8")
            partition_key = message.topic(), message.key()
            assert (
                observed_partitions.setdefault(partition_key, message.partition())
                == message.partition()
            )
            received.append((message.topic(), message.key(), message.value()))
        assert Counter(received) == expected_records
        assert Counter(topic for topic, _, _ in received) == {
            "config.assets": 3,
            "config.events": 8,
            "config.snapshots": 8,
        }
        assert consumer.poll(1) is None
    finally:
        consumer.close()


def test_unavailable_broker_exits_nonzero() -> None:
    with socket.socket() as unavailable:
        unavailable.bind(("127.0.0.1", 0))
        port = unavailable.getsockname()[1]
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "generator.main",
                "--assets",
                "1",
                "--events",
                "1",
                "--no-sleep",
                "--sink",
                "kafka",
            ],
            env={
                **os.environ,
                "KAFKA_BOOTSTRAP_SERVERS": f"127.0.0.1:{port}",
                "KAFKA_DELIVERY_TIMEOUT_MS": "500",
                "KAFKA_FLUSH_TIMEOUT_SECONDS": "2",
            },
            capture_output=True,
            text=True,
            timeout=10,
        )
    assert result.returncode == 2
    assert result.stdout == ""
    assert "Kafka delivery failed" in result.stderr
    assert "confirmed" not in result.stderr
