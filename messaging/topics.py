import logging
from dataclasses import dataclass

from messaging.config import KafkaConfig
from messaging.serialization import Record, record_type

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Topic:
    name: str
    partitions: int
    cleanup_policy: str = "delete"
    retention_ms: int = 604_800_000

    def configs(self) -> dict[str, str]:
        return {"cleanup.policy": self.cleanup_policy, "retention.ms": str(self.retention_ms)}


TOPICS = (
    Topic("config.assets", 3, "compact", -1),
    Topic("config.events", 6),
    Topic("config.snapshots", 6),
    Topic("config.alerts", 3),
    Topic("config.dlq", 3),
)
RECORD_TOPICS = {
    "asset": "config.assets",
    "change_event": "config.events",
    "config_snapshot": "config.snapshots",
}


def topic_for(record: Record) -> str:
    return RECORD_TOPICS[record_type(record)]


def ensure_topics(config: KafkaConfig) -> None:
    from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic, ResourceType

    admin = AdminClient(config.client_options(), logger=LOGGER)
    existing = admin.list_topics(timeout=10).topics
    missing = [
        NewTopic(topic.name, topic.partitions, 1, config=topic.configs())
        for topic in TOPICS
        if topic.name not in existing
    ]
    if missing:
        for future in admin.create_topics(missing, request_timeout=15).values():
            future.result(timeout=20)
    metadata = admin.list_topics(timeout=10).topics
    resources = [ConfigResource(ResourceType.TOPIC, topic.name) for topic in TOPICS]
    configurations = admin.describe_configs(resources, request_timeout=10)
    for topic, resource in zip(TOPICS, resources, strict=True):
        actual = metadata[topic.name]
        if actual.error or len(actual.partitions) != topic.partitions:
            raise RuntimeError(
                f"{topic.name}: expected {topic.partitions} partitions, no metadata errors"
            )
        if any(len(partition.replicas) != 1 for partition in actual.partitions.values()):
            raise RuntimeError(f"{topic.name}: expected replication factor 1 for local development")
        actual_configs = configurations[resource].result(timeout=15)
        for key, expected in topic.configs().items():
            if actual_configs[key].value != expected:
                raise RuntimeError(
                    f"{topic.name}: expected {key}={expected}; existing topic unchanged"
                )
        LOGGER.info("Topic ready: %s (%s partitions, RF=1)", topic.name, topic.partitions)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        ensure_topics(KafkaConfig.from_env())
    except ImportError:
        LOGGER.error(
            "Kafka support is not installed; run make setup or install configstream[kafka]"
        )
        return 2
    except Exception as error:
        LOGGER.error("Topic setup failed: %s", error)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
