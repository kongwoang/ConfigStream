import builtins
import io
import json
from unittest.mock import Mock

import pytest

from generator import main as cli
from generator.engine import WorkloadGenerator
from generator.serialization import serialize_record as serialize_jsonl
from messaging import kafka_producer
from messaging.config import KafkaConfig
from messaging.kafka_producer import KafkaProducer, KafkaPublishError
from messaging.serialization import serialize_record
from messaging.topics import TOPICS, topic_for
from schemas import Alert, Asset, ChangeEvent, ConfigSnapshot


@pytest.fixture
def client() -> Mock:
    return Mock(flush=Mock(return_value=0))


@pytest.fixture
def records() -> tuple[Asset, ChangeEvent, ConfigSnapshot]:
    generator = WorkloadGenerator(assets=1)
    return (generator.states[0].asset, *generator.next_change())


def test_config_defaults_and_idempotent_settings() -> None:
    config = KafkaConfig.from_env({})
    options = config.producer_options()
    assert options["bootstrap.servers"] == "localhost:9092"
    assert options["client.id"] == "configstream-generator"
    assert options["enable.idempotence"] is True
    assert options["acks"] == "all"
    assert options["max.in.flight.requests.per.connection"] == 5
    assert options["allow.auto.create.topics"] is False
    assert options["message.timeout.ms"] == 10000


def test_environment_loading_is_centralized(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
    monkeypatch.setenv("KAFKA_CLIENT_ID", "test-client")
    monkeypatch.setenv("KAFKA_DELIVERY_TIMEOUT_MS", "5000")
    monkeypatch.setenv("KAFKA_FLUSH_TIMEOUT_SECONDS", "6")
    monkeypatch.setenv("KAFKA_ENQUEUE_TIMEOUT_SECONDS", "2")
    assert KafkaConfig.from_env() == KafkaConfig("broker:29092", "test-client", 5000, 6, 2)


@pytest.mark.parametrize(
    "values",
    [
        {"KAFKA_BOOTSTRAP_SERVERS": " "},
        {"KAFKA_CLIENT_ID": ""},
        {"KAFKA_DELIVERY_TIMEOUT_MS": "0"},
        {"KAFKA_DELIVERY_TIMEOUT_MS": "invalid"},
        {"KAFKA_FLUSH_TIMEOUT_SECONDS": "-1"},
        {"KAFKA_FLUSH_TIMEOUT_SECONDS": "nan"},
        {"KAFKA_ENQUEUE_TIMEOUT_SECONDS": "inf"},
        {"KAFKA_ENQUEUE_TIMEOUT_SECONDS": "0"},
    ],
)
def test_invalid_config_is_rejected(values: dict[str, str]) -> None:
    with pytest.raises(ValueError):
        KafkaConfig.from_env(values)


def test_topic_specifications() -> None:
    assert {topic.name: topic.partitions for topic in TOPICS} == {
        "config.assets": 3,
        "config.events": 6,
        "config.snapshots": 6,
        "config.alerts": 3,
        "config.dlq": 3,
    }
    assert TOPICS[0].configs() == {"cleanup.policy": "compact", "retention.ms": "-1"}
    assert all(topic.retention_ms == 604800000 for topic in TOPICS[1:])


@pytest.mark.parametrize(
    "index,topic,record_type",
    [
        (0, "config.assets", "asset"),
        (1, "config.events", "change_event"),
        (2, "config.snapshots", "config_snapshot"),
    ],
)
def test_routing_envelope_utf8_key_and_callbacks(
    records: tuple, client: Mock, index: int, topic: str, record_type: str
) -> None:
    record = records[index]
    producer = KafkaProducer(KafkaConfig(), client=client)
    producer.publish(record)
    call = client.produce.call_args
    assert call.args == (topic,)
    assert topic_for(record) == topic
    assert call.kwargs["key"] == record.asset_id.encode("utf-8")
    assert isinstance(call.kwargs["value"], bytes)
    envelope = json.loads(call.kwargs["value"])
    assert envelope["record_type"] == record_type
    assert type(record).model_validate(envelope["payload"]) == record
    assert envelope["payload"]["schema_version"] == "1.0"
    call.kwargs["on_delivery"](None, Mock())
    assert producer.delivered == 1
    assert client.poll.call_count == 2
    client.flush.assert_not_called()
    assert "partition" not in call.kwargs


def test_serialization_stays_byte_compatible_with_jsonl(records: tuple) -> None:
    for record in records[1:]:
        assert serialize_record(record) == serialize_jsonl(record)
    asset = Asset(asset_id="máy-chủ", asset_type="generic_service")
    assert "máy-chủ" in serialize_record(asset)


def test_unsupported_types_are_not_routed(alert_payload: dict) -> None:
    with pytest.raises(TypeError):
        topic_for(Alert.model_validate(alert_payload))


def test_delivery_failure_is_logged_and_propagates(
    records: tuple, client: Mock, caplog: pytest.LogCaptureFixture
) -> None:
    producer = KafkaProducer(KafkaConfig(), client)
    producer.publish(records[0])
    callback = client.produce.call_args.kwargs["on_delivery"]
    callback("delivery timed out", Mock(topic=Mock(return_value="config.assets")))
    with pytest.raises(KafkaPublishError, match="delivery timed out"):
        producer.flush()
    with pytest.raises(KafkaPublishError):
        producer.publish(records[1])
    assert "Kafka delivery failed" in caplog.text
    assert producer.delivered == 0
    assert client.produce.call_count == 1


def test_flush_dispatches_delivery_failure(records: tuple, client: Mock) -> None:
    producer = KafkaProducer(KafkaConfig(), client)
    producer.publish(records[0])

    def fail_on_flush(timeout: float) -> int:
        client.produce.call_args.kwargs["on_delivery"](
            "failed", Mock(topic=lambda: "config.assets")
        )
        return 0

    client.flush.side_effect = fail_on_flush
    with pytest.raises(KafkaPublishError, match="failed"):
        producer.flush()


def test_nonzero_flush_is_not_reported_as_success(client: Mock) -> None:
    client.flush.return_value = 2
    with pytest.raises(KafkaPublishError, match="2 records unconfirmed"):
        KafkaProducer(KafkaConfig(), client).flush()


@pytest.mark.parametrize("method", ["produce", "poll", "flush"])
def test_synchronous_client_failures_propagate(records: tuple, client: Mock, method: str) -> None:
    getattr(client, method).side_effect = RuntimeError("broker unavailable")
    producer = KafkaProducer(KafkaConfig(), client)
    with pytest.raises(KafkaPublishError, match="broker unavailable"):
        if method == "flush":
            producer.flush()
        else:
            producer.publish(records[0])


def test_queue_full_polls_then_retries(records: tuple, client: Mock) -> None:
    client.produce.side_effect = [BufferError(), None]
    KafkaProducer(KafkaConfig(), client).publish(records[0])
    assert client.produce.call_count == 2
    assert client.produce.call_args_list[0] == client.produce.call_args_list[1]
    assert any(call.args[0] > 0 for call in client.poll.call_args_list)
    client.flush.assert_not_called()


def test_queue_full_has_bounded_wait(
    records: tuple, client: Mock, monkeypatch: pytest.MonkeyPatch
) -> None:
    client.produce.side_effect = BufferError()
    times = iter([0.0, 6.0])
    monkeypatch.setattr(kafka_producer, "monotonic", lambda: next(times))
    with pytest.raises(KafkaPublishError, match="enqueue timeout"):
        KafkaProducer(KafkaConfig(), client).publish(records[0])


def test_context_flushes_on_graceful_exit_and_interrupt(client: Mock) -> None:
    with KafkaProducer(KafkaConfig(), client):
        pass
    with pytest.raises(KeyboardInterrupt), KafkaProducer(KafkaConfig(), client):
        raise KeyboardInterrupt()
    assert client.flush.call_count == 2


def test_shutdown_does_not_hide_original_error(
    client: Mock, caplog: pytest.LogCaptureFixture
) -> None:
    client.flush.return_value = 2
    with pytest.raises(ValueError, match="original"), KafkaProducer(KafkaConfig(), client):
        raise ValueError("original")
    assert "Additional Kafka failure" in caplog.text


def test_real_client_factory_receives_idempotence_config(
    monkeypatch: pytest.MonkeyPatch, client: Mock
) -> None:
    import sys

    factory = Mock(return_value=client)
    monkeypatch.setitem(sys.modules, "confluent_kafka", Mock(Producer=factory))
    KafkaProducer(KafkaConfig())
    assert factory.call_args.args[0] == KafkaConfig().producer_options()


def test_kafka_workload_registers_assets_once_and_flushes_boundaries(
    monkeypatch: pytest.MonkeyPatch, client: Mock
) -> None:
    monkeypatch.setattr(
        kafka_producer, "KafkaProducer", lambda config: KafkaProducer(config, client)
    )
    generator = WorkloadGenerator(assets=3)
    cli.publish_workload(generator, events=8, paced=False)
    topics = [call.args[0] for call in client.produce.call_args_list]
    assert topics == ["config.assets"] * 3 + ["config.events", "config.snapshots"] * 8
    operations = [call[0] for call in client.method_calls if call[0] in {"produce", "flush"}]
    assert operations == ["produce"] * 3 + ["flush"] + ["produce"] * 16 + ["flush"]
    assert generator.sequence == 8


def test_asset_failure_prevents_generating_changes(
    monkeypatch: pytest.MonkeyPatch, client: Mock
) -> None:
    client.flush.return_value = 3
    monkeypatch.setattr(
        kafka_producer, "KafkaProducer", lambda config: KafkaProducer(config, client)
    )
    generator = WorkloadGenerator(assets=3)
    with pytest.raises(KafkaPublishError):
        cli.publish_workload(generator, events=8, paced=False)
    assert generator.sequence == 0


def test_cli_kafka_failure_is_nonzero_without_stdout(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    monkeypatch.setattr(
        cli, "publish_workload", Mock(side_effect=KafkaPublishError("broker unavailable"))
    )
    with pytest.raises(SystemExit) as error:
        cli.main(["--events", "1", "--sink", "kafka", "--no-sleep"])
    assert error.value.code == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "broker unavailable" in captured.err


def test_cli_rejects_file_output_for_kafka(tmp_path, capsys) -> None:
    output = tmp_path / "must-not-exist.jsonl"
    with pytest.raises(SystemExit) as error:
        cli.main(["--events", "1", "--sink", "kafka", "--output", str(output)])
    assert error.value.code == 2
    assert not output.exists()


def test_jsonl_and_missing_kafka_dependency(monkeypatch: pytest.MonkeyPatch) -> None:
    original_import = builtins.__import__

    def without_kafka(name, *args, **kwargs):
        if name.startswith("confluent_kafka"):
            raise ImportError("Kafka intentionally unavailable")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", without_kafka)
    output = io.StringIO()
    cli.write_workload(WorkloadGenerator(assets=1), 2, output, paced=False)
    assert len(output.getvalue().splitlines()) == 4
    with pytest.raises(KafkaPublishError, match="not installed"):
        KafkaProducer(KafkaConfig())
