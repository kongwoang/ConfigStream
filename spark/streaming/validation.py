from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as functions

from spark.common.schemas import KAFKA_COLUMNS, SNAPSHOT_COLUMNS

TIMESTAMP_PATTERN = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})$"


def nonblank(name: str) -> Column:
    return functions.coalesce(functions.col(name).rlike(r"(?U)\S"), functions.lit(False))


def classify_records(parsed: DataFrame) -> DataFrame:
    checks = [
        (functions.col("_malformed_json"), "malformed_json"),
        (~functions.col("record_type").eqNullSafe("config_snapshot"), "wrong_record_type"),
        (functions.col("_missing_payload"), "missing_payload"),
        (~functions.col("schema_version").eqNullSafe("1.0"), "unsupported_schema_version"),
        (~nonblank("snapshot_id"), "missing_snapshot_id"),
        (~nonblank("asset_id"), "missing_asset_id"),
        (functions.col("version").isNull() | (functions.col("version") <= 0), "invalid_version"),
        (~nonblank("config_format"), "invalid_config_format"),
    ]
    for name in ("event_time", "ingest_time"):
        checks.append(
            (
                functions.col(name).isNull()
                | ~functions.coalesce(
                    functions.col(f"{name}_raw").rlike(TIMESTAMP_PATTERN), functions.lit(False)
                ),
                f"invalid_{name}",
            )
        )
    checks.extend(
        [
            (
                ~functions.coalesce(
                    functions.col("hash").rlike(r"^[0-9a-f]{64}$"), functions.lit(False)
                ),
                "invalid_hash",
            ),
            (
                (functions.col("content").isNull() == functions.col("content_uri").isNull())
                | (functions.col("content_uri").isNotNull() & ~nonblank("content_uri")),
                "invalid_content_location",
            ),
            (
                ~functions.col("kafka_key").eqNullSafe(functions.col("asset_id")),
                "kafka_key_mismatch",
            ),
        ]
    )
    error = functions.coalesce(
        *[functions.when(condition, functions.lit(reason)) for condition, reason in checks]
    )
    return parsed.withColumn("validation_error", error).withColumn(
        "record_status",
        functions.when(functions.col("validation_error").isNull(), "valid").otherwise("invalid"),
    )


def valid_snapshots(classified: DataFrame) -> DataFrame:
    return classified.filter(functions.col("validation_error").isNull()).select(
        *SNAPSHOT_COLUMNS, *KAFKA_COLUMNS
    )


def console_rows(classified: DataFrame) -> DataFrame:
    return classified.select(
        "record_status",
        "validation_error",
        *[
            functions.when(functions.col("validation_error").isNull(), functions.col(name)).alias(
                name
            )
            for name in ("asset_id", "version", "snapshot_id", "event_time", "hash")
        ],
        *KAFKA_COLUMNS,
    )
