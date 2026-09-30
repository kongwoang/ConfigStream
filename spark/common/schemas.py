from pyspark.sql.types import LongType, StringType, StructField, StructType

SNAPSHOT_SCHEMA = StructType(
    [
        StructField("schema_version", StringType()),
        StructField("snapshot_id", StringType()),
        StructField("asset_id", StringType()),
        StructField("version", LongType()),
        StructField("config_format", StringType()),
        StructField("event_time", StringType()),
        StructField("ingest_time", StringType()),
        StructField("hash", StringType()),
        StructField("content", StringType()),
        StructField("content_uri", StringType()),
    ]
)

ENVELOPE_SCHEMA = StructType(
    [
        StructField("record_type", StringType()),
        StructField("payload", SNAPSHOT_SCHEMA),
        StructField("_corrupt_record", StringType()),
    ]
)

SNAPSHOT_COLUMNS = SNAPSHOT_SCHEMA.fieldNames()
KAFKA_COLUMNS = [
    "kafka_key",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
]
