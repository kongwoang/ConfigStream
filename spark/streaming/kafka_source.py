from pyspark.sql import DataFrame, SparkSession

from spark.common.config import SNAPSHOTS_TOPIC, SparkConfig


def read_snapshots(session: SparkSession, config: SparkConfig) -> DataFrame:
    return (
        session.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.bootstrap_servers)
        .option("subscribe", SNAPSHOTS_TOPIC)
        .option("startingOffsets", config.starting_offsets)
        .option("kafka.allow.auto.create.topics", "false")
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", config.max_offsets_per_trigger)
        .option("kafka.request.timeout.ms", "10000")
        .option("kafka.default.api.timeout.ms", "15000")
        .option("fetchOffset.numRetries", "1")
        .option("kafkaConsumer.pollTimeoutMs", "10000")
        .load()
    )
