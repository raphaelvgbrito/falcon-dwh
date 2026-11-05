"""External reference data, loaded by code: exchange rates and public holidays.

ref_fx_rates   grain: one row per (rate_date, currency), every calendar day of the run range
               units_per_usd = how many units of `currency` one USD buys; USD itself = 1.0
ref_holidays   grain: one row per (country_code, holiday_date)

Both are tiny. They are fetched once and kept as tables, so re-runs are reproducible and do not
depend on the API being up (set external.refresh: true to fetch again).

Locked-down clusters (e.g. Databricks serverless without internet) cannot reach the FX API. Every
successful fetch therefore also saves the raw API response to external.fx_cache_path (in the repo),
and a run that cannot reach the API loads that saved response instead, with a warning. The data
still comes from the API by code; the file is its recorded response. Refresh it with
    python -m falcon_dwh.external
"""
import datetime as dt
import json
import logging

import requests
from pyspark.sql import functions as F

from falcon_dwh.config import load_config, repo_path, table_name
from falcon_dwh.io import overwrite_table, table_exists

log = logging.getLogger(__name__)


def fetch_fx(url, base, start, end, timeout):
    """Raw frankfurter response: {"base", "start_date", "end_date", "rates": {day: {ccy: rate}}}."""
    resp = requests.get(f"{url.rstrip('/')}/{start}..{end}", params={"base": base}, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def parse_fx(body):
    """{date: {currency: units per base}} for the ECB business days in a frankfurter response."""
    return {dt.date.fromisoformat(d): rates for d, rates in body["rates"].items()}


def fx_range(cfg):
    ext, run = cfg["external"], cfg["run"]
    end = dt.date.fromisoformat(run["last_date"])
    start = dt.date.fromisoformat(run["first_date"]) - dt.timedelta(days=ext.get("fx_lookback_days", 10))
    return start, end


def get_fx_response(cfg, fetch=fetch_fx):
    """API response for the run range: live if reachable (and saved), else the saved response.
    Returns (body, source description)."""
    ext = cfg["external"]
    start, end = fx_range(cfg)
    cache = repo_path(ext["fx_cache_path"]) if ext.get("fx_cache_path") else None
    try:
        body = fetch(ext["fx_url"], ext["fx_base"], start, end, ext.get("http_timeout_s", 30))
    except requests.RequestException as e:
        if cache is None or not cache.exists():
            raise
        saved = json.loads(cache.read_text(encoding="utf-8"))
        if saved["request"]["start"] > str(start) or saved["request"]["end"] < str(end):
            raise RuntimeError(f"FX API unreachable and {cache} does not cover {start}..{end}") from e
        log.warning("FX API unreachable (%s); using the response saved at %s from %s",
                    type(e).__name__, saved["fetched_at_utc"], saved["request"]["url"])
        return saved["response"], f"{saved['request']['url']} (saved response, fetched {saved['fetched_at_utc']})"
    if cache is not None:
        try:
            save_fx_response(cache, ext["fx_url"], ext["fx_base"], start, end, body)
        except OSError as e:  # e.g. read-only checkout in a job: not a reason to fail the run
            log.warning("could not save FX response to %s: %s", cache, e)
    return body, ext["fx_url"]


def save_fx_response(path, url, base, start, end, body):
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "request": {"url": f"{url.rstrip('/')}/{start}..{end}?base={base}", "start": str(start), "end": str(end)},
        "fetched_at_utc": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "response": body,
    }
    path.write_text(json.dumps(record, indent=1, sort_keys=True), encoding="utf-8")


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
    start, end = fx_range(cfg)
    name = "ref_fx_rates"
    if table_exists(spark, cfg, name) and not ext.get("refresh"):
        got = spark.table(table_name(cfg, name)).agg(F.min("rate_date"), F.max("rate_date")).first()
        if got[0] is not None and got[0] <= dt.date.fromisoformat(run["first_date"]) and got[1] >= end:
            return  # already covers the whole run range
    log.info("loading FX %s..%s from %s", start, end, ext["fx_url"])
    body, source = get_fx_response(cfg)
    rows = fill_calendar(parse_fx(body), start, end, ext["fx_base"])
    df = spark.createDataFrame(rows, "rate_date date, currency string, units_per_usd double, source_rate_date date")
    overwrite_table(df.withColumn("source", F.lit(source)), cfg, name)


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


if __name__ == "__main__":
    # Fetch the FX rates for the configured range and save the response (no Spark needed).
    import sys

    logging.basicConfig(level=logging.INFO)
    cfg = load_config(sys.argv[1] if len(sys.argv) > 1 else None)
    body, source = get_fx_response(cfg)
    print(f"saved {len(body['rates'])} ECB days from {source} to {cfg['external']['fx_cache_path']}")
