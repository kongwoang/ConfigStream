from datetime import UTC, datetime
from hashlib import sha256
from typing import Any

import pytest
from pydantic import ValidationError

from schemas import ConfigSnapshot


def test_snapshot_preserves_content_and_converts_timestamps(
    snapshot_payload: dict[str, Any],
) -> None:
    snapshot = ConfigSnapshot.model_validate(snapshot_payload)
    assert snapshot.content == snapshot_payload["content"]
    assert snapshot.event_time == datetime(2026, 9, 30, 3, tzinfo=UTC)
    assert snapshot.event_time.tzinfo is UTC
    assert snapshot.ingest_time.tzinfo is UTC
    assert snapshot.config_format == "text"
    assert snapshot.content_uri is None
    assert "hostname" not in repr(snapshot)


@pytest.mark.parametrize("version", [0, -1, True, 1.5, "1", None])
def test_version_must_be_a_positive_integer(snapshot_payload: dict[str, Any], version: Any) -> None:
    with pytest.raises(ValidationError):
        ConfigSnapshot.model_validate({**snapshot_payload, "version": version})


@pytest.mark.parametrize("content", ["", "  hostname máy-chủ\n\n\tlogging enabled  \n"])
def test_empty_and_unicode_content_are_preserved(
    snapshot_payload: dict[str, Any], content: str
) -> None:
    snapshot_payload.update(content=content, hash=sha256(content.encode("utf-8")).hexdigest())
    snapshot = ConfigSnapshot.model_validate(snapshot_payload)
    assert snapshot.content == content


@pytest.mark.parametrize("digest", ["", "abc", "G" * 64, "0" * 64, None])
def test_invalid_or_mismatched_hash_is_rejected(
    snapshot_payload: dict[str, Any], digest: Any
) -> None:
    with pytest.raises(ValidationError):
        ConfigSnapshot.model_validate({**snapshot_payload, "hash": digest})


@pytest.mark.parametrize(
    "uri", ["s3://config-lake/raw/snap-001.txt", "https://example.org/snap.txt"]
)
def test_uri_snapshot_does_not_require_inline_content(
    snapshot_payload: dict[str, Any], uri: str
) -> None:
    snapshot_payload.pop("content")
    snapshot = ConfigSnapshot.model_validate({**snapshot_payload, "content_uri": uri})
    assert str(snapshot.content_uri) == uri
    assert snapshot.content is None
    assert ConfigSnapshot.model_validate_json(snapshot.model_dump_json()) == snapshot


@pytest.mark.parametrize(
    "uri",
    [
        "",
        "relative/path",
        "file:///tmp/config",
        "http://example.org/config",
        "s3://config-lake",
        "s3:///raw/config",
        "s3://config-lake/",
        "https://user:example@example.org/config",
        "https://example.org/config?signature=example",
        "s3://config-lake/raw/config#part",
    ],
)
def test_invalid_content_uri_is_rejected(snapshot_payload: dict[str, Any], uri: str) -> None:
    snapshot_payload.pop("content")
    with pytest.raises(ValidationError):
        ConfigSnapshot.model_validate({**snapshot_payload, "content_uri": uri})


@pytest.mark.parametrize("with_both", [False, True])
def test_snapshot_requires_exactly_one_content_location(
    snapshot_payload: dict[str, Any], with_both: bool
) -> None:
    if with_both:
        snapshot_payload["content_uri"] = "s3://config-lake/raw/snap-001.txt"
    else:
        snapshot_payload.pop("content")
    with pytest.raises(ValidationError, match="exactly one"):
        ConfigSnapshot.model_validate(snapshot_payload)


def test_binary_content_is_not_silently_decoded(snapshot_payload: dict[str, Any]) -> None:
    snapshot_payload["content"] = snapshot_payload["content"].encode()
    with pytest.raises(ValidationError):
        ConfigSnapshot.model_validate(snapshot_payload)
