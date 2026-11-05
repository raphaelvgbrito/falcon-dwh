"""External reference data, loaded by code: exchange rates and public holidays.

ref_fx_rates   grain: one row per (rate_date, currency), every calendar day of the run range
               units_per_usd = how many units of `currency` one USD buys; USD itself = 1.0
ref_holidays   grain: one row per (country_code, holiday_date)

Both are tiny. They are fetched once and kept as tables, so re-runs are reproducible and do not
depend on the API being up (set external.refresh: true to fetch again).
"""
import datetime as dt
import logging

import requests
from pyspark.sql import functions as F

from falcon_dwh.config import table_name
from falcon_dwh.io import overwrite_table, table_exists

log = logging.getLogger(__name__)


def fetch_fx(url, base, start, end, timeout):
    """{date: {currency: units per base}} for ECB business days between start and end."""
    resp = requests.get(f"{url.rstrip('/')}/{start}..{end}", params={"base": base}, timeout=timeout)
    resp.raise_for_status()
    body = resp.json()
    return {dt.date.fromisoformat(d): rates for d, rates in body["rates"].items()}


def fill_calendar(rates_by_day, start, end, base="USD"):
    """Rate for every calendar day = the latest published rate ON OR BEFORE that day.

    Weekends and holidays reuse the previous business day; a day never uses a later rate, so the
    conversion never looks into the future. Days before the first published rate get nothing.
    """
    rows, current = [], {}
    published = sorted(rates_by_day)
    i = 0
    day = start
    while day <= end:
        while i < len(published) and published[i] <= day:
            current = dict(rates_by_day[published[i]])
            source_day = published[i]
            i += 1
        if current:
            rows.append((day, base, 1.0, day))
            rows.extend((day, ccy, float(rate), source_day) for ccy, rate in sorted(current.items()))
        day += dt.timedelta(days=1)
    return rows


def load_fx_rates(spark, cfg):
    ext, run = cfg["external"], cfg["run"]
    end = dt.date.fromisoformat(run["last_date"])
    start = dt.date.fromisoformat(run["first_date"]) - dt.timedelta(days=ext.get("fx_lookback_days", 10))
    name = "ref_fx_rates"
    if table_exists(spark, cfg, name) and not ext.get("refresh"):
        got = spark.table(table_name(cfg, name)).agg(F.min("rate_date"), F.max("rate_date")).first()
        if got[0] is not None and got[0] <= dt.date.fromisoformat(run["first_date"]) and got[1] >= end:
            return  # already covers the whole run range
    log.info("fetching FX %s..%s from %s", start, end, ext["fx_url"])
    rates = fetch_fx(ext["fx_url"], ext["fx_base"], start, end, ext.get("http_timeout_s", 30))
    rows = fill_calendar(rates, start, end, ext["fx_base"])
    df = spark.createDataFrame(rows, "rate_date date, currency string, units_per_usd double, source_rate_date date")
    overwrite_table(df.withColumn("source", F.lit(ext["fx_url"])), cfg, name)


def holiday_rows(country_codes, years):
    """(country, date, name) for every supported country; unsupported codes are skipped."""
    import holidays

    rows, unsupported = [], []
    for code in sorted(c for c in country_codes if c):
        try:
            # English names where the package has them (otherwise its default language)
            cal = holidays.country_holidays(code, years=years, language="en_US")
        except NotImplementedError:
            unsupported.append(code)
            continue
        rows.extend((code, d, name) for d, name in sorted(cal.items()))
    return rows, unsupported


def load_holidays(spark, cfg):
    name = "ref_holidays"
    if table_exists(spark, cfg, name) and not cfg["external"].get("refresh"):
        return
    run = cfg["run"]
    years = sorted({int(run["first_date"][:4]), int(run["last_date"][:4])})
    # ~200 distinct country codes: a tiny result, safe to bring to the driver
    codes = [r[0] for r in spark.table(table_name(cfg, "stg_requests")).select("country").distinct().collect()]
    rows, unsupported = holiday_rows(codes, years)
    log.info("holidays: %d rows, %d countries not supported by the package: %s",
             len(rows), len(unsupported), ",".join(unsupported))
    df = spark.createDataFrame(rows, "country_code string, holiday_date date, holiday_name string")
    overwrite_table(df, cfg, name)


def load_reference_data(spark, cfg):
    load_fx_rates(spark, cfg)
    load_holidays(spark, cfg)
