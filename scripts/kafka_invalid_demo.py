"""Publish deliberately invalid synthetic fixtures to the local development topic."""

import json
import logging
from typing import Any

from generator.engine import WorkloadGenerator
from messaging.config import KafkaConfig
from spark.common.config import SNAPSHOTS_TOPIC

LOGGER = logging.getLogger(__name__)


def publish_invalid_records(config: KafkaConfig) -> int:
    from confluent_kafka import Producer

    snapshot = WorkloadGenerator(assets=1, seed=42).next_change()[1]
    payload = snapshot.model_dump(mode="json")
    envelope = {"record_type": "config_snapshot", "payload": payload}
    missing_asset = {name: value for name, value in payload.items() if name != "asset_id"}
    records = [
        (snapshot.asset_id, '{"content":"SENSITIVE_TEST_MARKER"'),
        (snapshot.asset_id, json.dumps({**envelope, "record_type": "change_event"})),
        (snapshot.asset_id, json.dumps({**envelope, "payload": missing_asset})),
        ("wrong-test-key", json.dumps(envelope)),
    ]
    failures = []
    delivered = 0

    def on_delivery(error: Any, message: Any) -> None:
        nonlocal delivered
        if error:
            failures.append(str(error))
        else:
            delivered += 1

    producer = Producer(config.producer_options(), logger=LOGGER)
    for key, value in records:
        producer.produce(
            SNAPSHOTS_TOPIC, key=key.encode(), value=value.encode(), on_delivery=on_delivery
        )
        producer.poll(0)
    remaining = producer.flush(config.flush_timeout_seconds)
    if remaining or failures or delivered != len(records):
        raise RuntimeError(
            f"Invalid-fixture delivery failed: remaining={remaining}, errors={failures}"
        )
    LOGGER.info("Published %s intentionally invalid test records", delivered)
    return delivered


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    publish_invalid_records(KafkaConfig.from_env())
