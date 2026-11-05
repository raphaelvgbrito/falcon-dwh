"""Staging layer: load one day folder into stg_<source> tables, exactly as delivered.

Each stg table = the source columns + load_date (the folder date) + _source_file.
Partitioned by load_date. A run replaces only that day's partition, so re-running a day is
idempotent: the partition is rewritten with the same rows, nothing is appended twice.
"""
import logging
from concurrent.futures import ThreadPoolExecutor

from pyspark.errors import AnalysisException
from pyspark.sql import functions as F

from falcon_dwh.config import day_path, table_name
from falcon_dwh.schemas import CORRUPT, SOURCES

log = logging.getLogger(__name__)


def read_source(spark, path, fmt, schema):
    reader = spark.read.schema(schema)
    if fmt == "csv":
        reader = (
            reader.option("header", True)
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", CORRUPT)
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
            .option("multiLine", True)  # quoted values may contain newlines
            .option("escape", '"')  # Postgres COPY CSV escapes quotes by doubling them
            .csv(path)
        )
    elif fmt == "json":
        reader = (
            reader.option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", CORRUPT)
            .json(path)
        )
    elif fmt == "parquet":
        reader = reader.parquet(path)
    else:
        raise ValueError(fmt)
    return reader


def stage_source(spark, cfg, name, load_date):
    """Load one source for one day. Returns the number of rows written, or None if missing."""
    fmt, rel_path, schema, _kind = SOURCES[name]
    path = f"{day_path(cfg, load_date)}/{rel_path}"
    try:
        df = read_source(spark, path, fmt, schema)
        df = df.select(
            "*",
            F.lit(load_date).cast("date").alias("load_date"),
            F.col("_metadata.file_path").alias("_source_file"),
        )
        target = table_name(cfg, f"stg_{name}")
        (
            df.write.format(cfg["warehouse"]["format"])
            .mode("overwrite")
            .option("replaceWhere", f"load_date = DATE'{load_date}'")
            .partitionBy("load_date")
            .saveAsTable(target)
        )
    except AnalysisException as e:
        if "PATH_NOT_FOUND" in str(e):
            log.warning("missing input %s for %s: %s", name, load_date, path)
            return None
        raise
    return spark.table(target).where(F.col("load_date") == F.lit(load_date).cast("date")).count()


def stage_day(spark, cfg, load_date, workers=8):
    """Stage every source of one day folder. Returns {source: rows written or None}.

    Sources are independent, so they are written in parallel threads (each is its own Spark job).
    """
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {name: pool.submit(stage_source, spark, cfg, name, load_date) for name in SOURCES}
        counts = {name: f.result() for name, f in futures.items()}
    for name, n in counts.items():
        log.info("staged %-20s %s rows=%s", name, load_date, n)
    return counts
