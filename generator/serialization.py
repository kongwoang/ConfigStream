from messaging.serialization import serialize_record as serialize_transport_record
from schemas import ChangeEvent, ConfigSnapshot


def serialize_record(record: ChangeEvent | ConfigSnapshot) -> str:
    if not isinstance(record, (ChangeEvent, ConfigSnapshot)):
        raise TypeError("only ChangeEvent and ConfigSnapshot records can be serialized")
    return serialize_transport_record(record)


def serialize_change(event: ChangeEvent, snapshot: ConfigSnapshot) -> str:
    return serialize_record(event) + serialize_record(snapshot)
