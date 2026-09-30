import argparse
import logging
import signal
from threading import Event
from types import FrameType
from typing import TYPE_CHECKING

from spark.common.config import SNAPSHOTS_TOPIC, SparkConfig
from spark.common.runtime import create_session

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.streaming import StreamingQuery

LOGGER = logging.getLogger(__name__)


def start_console(
    classified: "DataFrame", config: SparkConfig, *, available_now: bool = False
) -> "StreamingQuery":
    from spark.streaming.validation import console_rows

    writer = (
        console_rows(classified)
        .writeStream.format("console")
        .outputMode("append")
        .queryName("config_snapshots_console")
        .option("checkpointLocation", str(config.checkpoint_dir.resolve()))
        .option("truncate", "false")
        .option("numRows", config.console_rows)
    )
    if available_now:
        return writer.trigger(availableNow=True).start()
    return writer.trigger(processingTime=f"{config.trigger_seconds} seconds").start()


def monitor_query(
    query: "StreamingQuery", *, available_now: bool, stop_requested: Event | None = None
) -> None:
    last_progress = None
    while True:
        if stop_requested is not None and stop_requested.is_set():
            raise KeyboardInterrupt
        terminated = query.awaitTermination(1)
        progress = query.lastProgress
        signature = (progress["batchId"], progress["numInputRows"]) if progress else None
        if progress and signature != last_progress:
            last_progress = signature
            LOGGER.info(
                "Progress batchId=%s numInputRows=%s inputRowsPerSecond=%s "
                "processedRowsPerSecond=%s",
                progress["batchId"],
                progress["numInputRows"],
                progress["inputRowsPerSecond"],
                progress["processedRowsPerSecond"],
            )
        if terminated:
            if not available_now:
                raise RuntimeError("Streaming query terminated unexpectedly")
            return


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read and validate Kafka snapshots using Spark")
    parser.add_argument(
        "--available-now", action="store_true", help="drain available data and exit"
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    logging.getLogger("py4j").setLevel(logging.WARNING)
    session = None
    query = None
    stop_requested = Event()
    previous_handlers = {
        signum: signal.getsignal(signum) for signum in (signal.SIGINT, signal.SIGTERM)
    }

    def request_shutdown(signum: int, frame: FrameType | None) -> None:
        stop_requested.set()

    try:
        config = SparkConfig.from_env()
        session = create_session(config)
        for signum in previous_handlers:
            signal.signal(signum, request_shutdown)
        from spark.streaming.kafka_source import read_snapshots
        from spark.streaming.parser import parse_records
        from spark.streaming.validation import classify_records

        LOGGER.info(
            "Subscribing to %s; startingOffsets=%s; checkpoint=%s; console limit=%s rows/batch",
            SNAPSHOTS_TOPIC,
            config.starting_offsets,
            config.checkpoint_dir.resolve(),
            config.console_rows,
        )
        classified = classify_records(parse_records(read_snapshots(session, config)))
        query = start_console(classified, config, available_now=args.available_now)
        LOGGER.info("Query started: %s (%s)", query.name, query.id)
        monitor_query(query, available_now=args.available_now, stop_requested=stop_requested)
    except KeyboardInterrupt:
        LOGGER.info("Shutdown requested")
    except Exception:
        LOGGER.exception("Spark streaming failed")
        return 1
    finally:
        if query is not None and query.isActive:
            query.stop()
        if session is not None:
            session.stop()
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)
        LOGGER.info("Query stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
