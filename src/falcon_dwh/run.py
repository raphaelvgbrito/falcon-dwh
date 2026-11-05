"""Entry point: process one day, or a range of days in order, like a scheduled job would.

    python -m falcon_dwh.run --date 2026-09-11
    python -m falcon_dwh.run --start 2026-08-24 --end 2026-10-25
"""
import argparse
import datetime as dt
import logging

from falcon_dwh.config import load_config
from falcon_dwh.dims import build_dims
from falcon_dwh.dq import build_quality
from falcon_dwh.external import load_reference_data
from falcon_dwh.facts import affected_impression_dates, build_facts
from falcon_dwh.mart import build_ml_mart
from falcon_dwh.spark_session import get_spark
from falcon_dwh.staging import stage_day

LAYERS = ("staging", "dims", "facts", "mart", "quality")

log = logging.getLogger("falcon_dwh")


def daterange(start, end):
    day = dt.date.fromisoformat(start)
    last = dt.date.fromisoformat(end)
    while day <= last:
        yield day.isoformat()
        day += dt.timedelta(days=1)


def run_day(spark, cfg, load_date, layers=LAYERS):
    """One scheduled run: stage the day folder, then rebuild what it can change downstream."""
    log.info("=== run %s layers=%s", load_date, ",".join(layers))
    if "staging" in layers:
        stage_day(spark, cfg, load_date)
    if "dims" in layers:
        build_dims(spark, cfg)
        log.info("dims rebuilt")
    touched = {}
    if "facts" in layers:
        touched = build_facts(spark, cfg, load_date)
        for table, dates in touched.items():
            log.info("%s rebuilt partitions: %s", table, ",".join(str(d) for d in sorted(dates)) or "-")
    if "mart" in layers:
        load_reference_data(spark, cfg)
        # the mart has the same grain as fact_impression, so it changes exactly where the fact did
        dates = touched.get("fact_impression") or affected_impression_dates(spark, cfg, load_date)
        build_ml_mart(spark, cfg, dates)
        log.info("ml_training rebuilt partitions: %s", ",".join(str(d) for d in sorted(dates)) or "-")
    if "quality" in layers:
        build_quality(spark, cfg, load_date)
        log.info("dq_report and recon_impressions written for %s", load_date)


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--config")
    p.add_argument("--date", help="process a single day (YYYY-MM-DD)")
    p.add_argument("--start")
    p.add_argument("--end")
    p.add_argument("--layers", default=",".join(LAYERS), help="comma list, default: all")
    args = p.parse_args(argv)
    layers = tuple(x.strip() for x in args.layers.split(","))

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    for noisy in ("py4j", "databricks", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    cfg = load_config(args.config)
    spark = get_spark(cfg)
    if args.date:
        days = [args.date]
    else:
        days = list(daterange(args.start or cfg["run"]["first_date"], args.end or cfg["run"]["last_date"]))
    for day in days:
        run_day(spark, cfg, day, layers)


if __name__ == "__main__":
    main()
