import json

from schemas import ChangeEvent, ConfigSnapshot


def serialize_record(record: ChangeEvent | ConfigSnapshot) -> str:
    if isinstance(record, ChangeEvent):
        record_type = "change_event"
    elif isinstance(record, ConfigSnapshot):
        record_type = "config_snapshot"
    else:
        raise TypeError("only ChangeEvent and ConfigSnapshot records can be serialized")
    return (
        json.dumps(
            {"record_type": record_type, "payload": record.model_dump(mode="json")},
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
    )


def serialize_change(event: ChangeEvent, snapshot: ConfigSnapshot) -> str:
    return serialize_record(event) + serialize_record(snapshot)
