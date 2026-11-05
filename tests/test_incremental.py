"""Integration test on real day folders: late events update earlier partitions, and re-running a
day changes nothing. Runs the real pipeline into the test schema, starting from empty tables."""
import pytest
from pyspark.sql import functions as F

from falcon_dwh.checks import fingerprint
from falcon_dwh.config import table_name
from falcon_dwh.run import run_day

# Impression shown 2026-08-24 (folder 08-24); its postback arrives in folder 2026-08-26.
LATE_FID = "YzVmZjhmZDU1NzM5MDJhYzow"
FACTS = [
    "fact_request", "fact_impression", "fact_postback", "rejected_impressions", "dim_placement", "dim_banner", "dim_date", "dim_time",
    "ml_training", "dq_report", "recon_impressions",
]


@pytest.fixture(scope="module")
def clean_schema(spark, cfg):
    wh = cfg["warehouse"]
    for t in spark.sql(f"SHOW TABLES IN {wh['catalog']}.{wh['schema']}").collect():
        spark.sql(f"DROP TABLE IF EXISTS {wh['catalog']}.{wh['schema']}.{t.tableName}")
    return cfg


def _impression(spark, cfg):
    return spark.table(table_name(cfg, "fact_impression")).where(F.col("fid") == LATE_FID).first()


def test_late_postback_updates_earlier_impression_and_rerun_is_stable(spark, clean_schema):
    cfg = clean_schema
    run_day(spark, cfg, "2026-08-24")
    before = _impression(spark, cfg)
    assert before is not None and str(before.impression_date) == "2026-08-24"
    assert before.is_conversion is False

    run_day(spark, cfg, "2026-08-25")
    run_day(spark, cfg, "2026-08-26")
    after = _impression(spark, cfg)
    assert after.is_conversion is True and after.revenue_usd > 0
    assert after.impression_date == before.impression_date

    prints = {t: fingerprint(spark.table(table_name(cfg, t))) for t in FACTS}
    run_day(spark, cfg, "2026-08-26")  # re-run the last day
    run_day(spark, cfg, "2026-08-24")  # and an earlier one, after later days were loaded
    assert {t: fingerprint(spark.table(table_name(cfg, t))) for t in FACTS} == prints
