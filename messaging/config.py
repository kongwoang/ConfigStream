import math
import os
from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class KafkaConfig:
    bootstrap_servers: str = "localhost:9092"
    client_id: str = "configstream-generator"
    delivery_timeout_ms: int = 10_000
    flush_timeout_seconds: float = 15.0
    enqueue_timeout_seconds: float = 5.0

    def __post_init__(self) -> None:
        if not self.bootstrap_servers.strip() or not self.client_id.strip():
            raise ValueError("Kafka bootstrap servers and client ID must not be empty")
        if type(self.delivery_timeout_ms) is not int or self.delivery_timeout_ms < 1:
            raise ValueError("KAFKA_DELIVERY_TIMEOUT_MS must be a positive integer")
        for value in (self.flush_timeout_seconds, self.enqueue_timeout_seconds):
            if not math.isfinite(value) or value <= 0:
                raise ValueError("Kafka flush/enqueue timeouts must be positive finite seconds")

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "KafkaConfig":
        values = os.environ if environ is None else environ
        return cls(
            bootstrap_servers=values.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            client_id=values.get("KAFKA_CLIENT_ID", "configstream-generator"),
            delivery_timeout_ms=int(values.get("KAFKA_DELIVERY_TIMEOUT_MS", "10000")),
            flush_timeout_seconds=float(values.get("KAFKA_FLUSH_TIMEOUT_SECONDS", "15")),
            enqueue_timeout_seconds=float(values.get("KAFKA_ENQUEUE_TIMEOUT_SECONDS", "5")),
        )

    def client_options(self) -> dict[str, str]:
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
            "broker.address.family": "v4",
        }

    def producer_options(self) -> dict[str, str | int | bool]:
        return {
            **self.client_options(),
            "enable.idempotence": True,
            "acks": "all",
            "max.in.flight.requests.per.connection": 5,
            "message.timeout.ms": self.delivery_timeout_ms,
            "linger.ms": 5,
            "allow.auto.create.topics": False,
        }
