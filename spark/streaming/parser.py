from pyspark.sql import DataFrame
from pyspark.sql import functions as functions

from spark.common.schemas import ENVELOPE_SCHEMA, KAFKA_COLUMNS


def parse_records(records: DataFrame) -> DataFrame:
    decoded = records.select(
        functions.col("key").cast("string").alias("kafka_key"),
        functions.col("value").cast("string").alias("_json"),
        functions.col("topic").alias("kafka_topic"),
        functions.col("partition").alias("kafka_partition"),
        functions.col("offset").alias("kafka_offset"),
        functions.col("timestamp").alias("kafka_timestamp"),
    )
    envelope = functions.from_json(
        functions.col("_json"),
        ENVELOPE_SCHEMA,
        {
            "mode": "PERMISSIVE",
            "columnNameOfCorruptRecord": "_corrupt_record",
            "allowSingleQuotes": "false",
            "allowNonNumericNumbers": "false",
        },
    )
    parsed = decoded.withColumn("_envelope", envelope).select(
        *KAFKA_COLUMNS,
        (
            functions.col("_envelope").isNull()
            | functions.col("_envelope._corrupt_record").isNotNull()
        ).alias("_malformed_json"),
        functions.col("_envelope.payload").isNull().alias("_missing_payload"),
        functions.col("_envelope.record_type").alias("record_type"),
        "_envelope.payload.*",
    )
    for name in ("event_time", "ingest_time"):
        parsed = parsed.withColumnRenamed(name, f"{name}_raw").withColumn(
            name,
            functions.try_to_timestamp(
                functions.col(f"{name}_raw"),
                functions.lit("yyyy-MM-dd'T'HH:mm:ss[.SSSSSSSSS]XXX"),
            ),
        )
    return parsed
