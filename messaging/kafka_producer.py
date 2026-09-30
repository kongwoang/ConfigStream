import logging
from time import monotonic
from types import TracebackType
from typing import Any, Self

from messaging.config import KafkaConfig
from messaging.serialization import Record, serialize_record
from messaging.topics import topic_for

LOGGER = logging.getLogger(__name__)


class KafkaPublishError(RuntimeError):
    """A record was not confirmed delivered; replay may be necessary."""


class KafkaProducer:
    def __init__(self, config: KafkaConfig, client: Any = None) -> None:
        self.config = config
        self._delivery_error: str | None = None
        self.delivered = 0
        if client is None:
            try:
                from confluent_kafka import Producer
            except ImportError as error:
                raise KafkaPublishError("Kafka support is not installed; run make setup") from error
            try:
                client = Producer(config.producer_options(), logger=LOGGER)
            except Exception as error:
                raise KafkaPublishError(f"Could not initialize Kafka producer: {error}") from error
        self._client = client

    def _on_delivery(self, error: Any, message: Any) -> None:
        if error is not None:
            self._delivery_error = self._delivery_error or f"{message.topic()}: {error}"
            LOGGER.error("Kafka delivery failed: %s", self._delivery_error)
        else:
            self.delivered += 1

    def _raise_delivery_error(self) -> None:
        if self._delivery_error:
            raise KafkaPublishError(f"Kafka delivery failed: {self._delivery_error}")

    def publish(self, record: Record) -> None:
        topic = topic_for(record)
        value = serialize_record(record).encode("utf-8")
        key = record.asset_id.encode("utf-8")
        deadline = monotonic() + self.config.enqueue_timeout_seconds
        try:
            self._client.poll(0)
            self._raise_delivery_error()
            while True:
                try:
                    self._client.produce(topic, key=key, value=value, on_delivery=self._on_delivery)
                    break
                except BufferError:
                    remaining = deadline - monotonic()
                    if remaining <= 0:
                        raise KafkaPublishError(
                            "Kafka producer queue is full; enqueue timeout"
                        ) from None
                    self._client.poll(min(0.1, remaining))
                    self._raise_delivery_error()
            self._client.poll(0)
            self._raise_delivery_error()
        except KafkaPublishError:
            raise
        except Exception as error:
            LOGGER.error("Kafka publish failed for %s: %s", topic, error)
            raise KafkaPublishError(f"Kafka publish failed for {topic}: {error}") from error

    def flush(self) -> None:
        try:
            remaining = self._client.flush(self.config.flush_timeout_seconds)
        except Exception as error:
            LOGGER.error("Kafka flush failed: %s", error)
            raise KafkaPublishError(f"Kafka flush failed: {error}") from error
        self._raise_delivery_error()
        if remaining:
            LOGGER.error("Kafka flush timed out: %s records unconfirmed", remaining)
            raise KafkaPublishError(f"Kafka flush timed out: {remaining} records unconfirmed")

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        error_type: type[BaseException] | None,
        error: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        try:
            self.flush()
        except KafkaPublishError:
            if error is None:
                raise
            LOGGER.exception("Additional Kafka failure during shutdown")
