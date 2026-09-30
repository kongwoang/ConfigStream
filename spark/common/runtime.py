import logging
import os
import re
import shutil
import subprocess
import sys
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import TYPE_CHECKING

from spark.common.config import KAFKA_CONNECTOR, SPARK_VERSION, SparkConfig

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

LOGGER = logging.getLogger(__name__)


def check_runtime() -> None:
    java_home = os.environ.get("JAVA_HOME")
    java = str(Path(java_home) / "bin/java") if java_home else shutil.which("java")
    if not java:
        raise RuntimeError("Java 21 is required; set JAVA_HOME or put java on PATH")
    try:
        result = subprocess.run([java, "-version"], capture_output=True, text=True, timeout=10)
    except (OSError, subprocess.TimeoutExpired) as error:
        raise RuntimeError(
            "Cannot run Java; check JAVA_HOME and java -version (requires 21)"
        ) from error
    output = result.stderr + result.stdout
    if result.returncode or not re.search(r'version "21(?:\.|\")', output):
        raise RuntimeError("ConfigStream Spark requires Java 21; check JAVA_HOME and java -version")
    try:
        installed = version("pyspark")
    except PackageNotFoundError as error:
        raise RuntimeError("PySpark is not installed; run make spark-setup") from error
    if installed != SPARK_VERSION:
        raise RuntimeError(
            f"Expected PySpark {SPARK_VERSION}, found {installed}; run make spark-setup"
        )


def create_session(config: SparkConfig, *, kafka: bool = True) -> "SparkSession":
    check_runtime()
    from pyspark.sql import SparkSession

    runtime = config.runtime_dir.resolve()
    for name in ("tmp", "local", "ivy", "warehouse"):
        (runtime / name).mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
    os.environ["SPARK_LOCAL_DIRS"] = str(runtime / "local")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["TMPDIR"] = str(runtime / "tmp")
    builder = (
        SparkSession.builder.master(config.master)
        .appName(config.app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.timestampType", "TIMESTAMP_LTZ")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.warehouse.dir", (runtime / "warehouse").as_uri())
        .config("spark.jars.ivy", str(runtime / "ivy"))
        .config("spark.driver.extraJavaOptions", f'-Djava.io.tmpdir="{runtime / "tmp"}"')
    )
    if kafka:
        builder = builder.config("spark.jars.packages", KAFKA_CONNECTOR)
    LOGGER.info("Starting Spark %s on %s (UTC)", SPARK_VERSION, config.master)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    if session.version != SPARK_VERSION:
        session.stop()
        raise RuntimeError(f"Spark runtime must match connector version {SPARK_VERSION}")
    return session
