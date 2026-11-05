"""Integration tests for the staging layer on a real day folder (needs the dataset + Spark)."""
from pyspark.sql import functions as F

from falcon_dwh.checks import fingerprint
from falcon_dwh.config import table_name
from falcon_dwh.schemas import CORRUPT, SOURCES
from falcon_dwh.staging import stage_day, stage_source

DAY = "2026-09-11"


def _partition(spark, cfg, name, day=DAY):
    return spark.table(table_name(cfg, f"stg_{name}")).where(F.col("load_date") == F.lit(day).cast("date"))


def test_staged_columns_are_the_explicit_schema(spark, cfg):
    stage_source(spark, cfg, "impressions", DAY)
    cols = _partition(spark, cfg, "impressions").columns
    expected = [f.name for f in SOURCES["impressions"][2].fields] + ["load_date", "_source_file"]
    assert cols == expected


def test_rerunning_a_day_does_not_change_staging(spark, cfg):
    stage_day(spark, cfg, DAY)
    before = {n: fingerprint(_partition(spark, cfg, n)) for n in SOURCES}
    stage_day(spark, cfg, DAY)
    after = {n: fingerprint(_partition(spark, cfg, n)) for n in SOURCES}
    assert before == after
    assert all(fp["rows"] > 0 for fp in after.values())


def test_rerun_only_replaces_its_own_partition(spark, cfg):
    other = "2026-09-12"
    stage_source(spark, cfg, "clicks", DAY)
    stage_source(spark, cfg, "clicks", other)
    before = fingerprint(_partition(spark, cfg, "clicks", other))
    # sanity: the fingerprint does tell two different days apart
    assert fingerprint(_partition(spark, cfg, "clicks", DAY)) != before
    stage_source(spark, cfg, "clicks", DAY)
    assert fingerprint(_partition(spark, cfg, "clicks", other)) == before


def test_missing_day_folder_is_reported_not_failed(spark, cfg):
    # 2026-10-03 is not in the delivered dataset
    assert stage_source(spark, cfg, "clicks", "2026-10-03") is None


def test_campaign_weight_decimal_is_not_corrupt(spark, cfg):
    stage_source(spark, cfg, "campaign", DAY)
    assert _partition(spark, cfg, "campaign").where(F.col(CORRUPT).isNotNull()).count() == 0
