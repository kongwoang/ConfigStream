import json

from schemas import Asset, ChangeEvent, ConfigSnapshot

type Record = Asset | ChangeEvent | ConfigSnapshot

RECORD_TYPES = {Asset: "asset", ChangeEvent: "change_event", ConfigSnapshot: "config_snapshot"}


def record_type(record: Record) -> str:
    try:
        return RECORD_TYPES[type(record)]
    except KeyError as error:
        raise TypeError("unsupported transport record type") from error


def serialize_record(record: Record) -> str:
    return (
        json.dumps(
            {"record_type": record_type(record), "payload": record.model_dump(mode="json")},
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
    )
