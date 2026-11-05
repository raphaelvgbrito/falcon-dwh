"""Unit tests for the ML mart pieces: user agent parsing, FX, holidays, history features."""
import datetime as dt

import pytest
from pyspark.sql import functions as F

from falcon_dwh.external import fill_calendar, holiday_rows
from falcon_dwh.history import rolling_rates
from falcon_dwh.mart import add_usd_amount
from falcon_dwh.transforms import parse_user_agent

D = dt.date.fromisoformat

UA_CASES = [
    # (user agent, device, os, os_major, browser) -- real shapes from the requests table
    ("Mozilla/5.0 (iPhone; CPU iPhone OS 18_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) "
     "Version/18.6 Mobile/15E148 Safari/604.1", "mobile", "iOS", 18, "Safari"),
    ("Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 "
     "Mobile Safari/537.36", "mobile", "Android", 10, "Chrome"),
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 "
     "Safari/537.36 Edg/139.0.0.0", "desktop", "Windows", None, "Edge"),
    ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) "
     "Version/26.6.1 Safari/605.1.15", "desktop", "macOS", None, "Safari"),
    ("Mozilla/5.0 (iPad; CPU OS 18_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) "
     "Version/18.5 Mobile/15E148 Safari/604.1", "tablet", "iOS", 18, "Safari"),
    ("app/1.0 app/12.24.30-196819 (env: prod; platform: ios; osVersion: 26.6.2; brand: apple; model: iPhone)",
     "mobile", "iOS", 26, "publisher app"),
    ("Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) "
     "Mobile/22F76 [FBAN/FBIOS;FBAV/520.0.0.38.101]", "mobile", "iOS", 18, "Facebook in-app"),
    ("Mozilla/5.0 (Linux; Android 14; SM-S918U Build/UP1A.231005.007; wv) AppleWebKit/537.36 "
     "(KHTML, like Gecko) Version/4.0 Chrome/139.0.0.0 Mobile Safari/537.36", "mobile", "Android", 14,
     "Android WebView"),
    ("Mozilla/5.0 (Android 15; Mobile; rv:142.0) Gecko/142.0 Firefox/142.0", "mobile", "Android", 15, "Firefox"),
    ("Mozilla/5.0 (X11; CrOS x86_64 14541.0.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 "
     "Safari/537.36", "desktop", "ChromeOS", None, "Chrome"),
    ("Dalvik/2.1.0 (Linux; U; Android 16; moto g - 2025 Build/W1VKS36H.9-12-10-11)",
     "mobile", "Android", 16, "native app"),
    (None, "unknown", "other", None, "unknown"),
]


def test_parse_user_agent(spark):
    df = spark.createDataFrame([(c[0],) for c in UA_CASES], "ua string")
    got = [tuple(r) for r in df.select("ua", *parse_user_agent(F.col("ua"))).collect()]
    assert got == [tuple(c) for c in UA_CASES]


def test_fx_fill_forward_never_uses_a_later_rate():
    published = {D("2026-08-21"): {"EUR": 0.85}, D("2026-08-24"): {"EUR": 0.86}}  # Fri, Mon
    rows = fill_calendar(published, D("2026-08-20"), D("2026-08-24"))
    eur = {r[0]: (r[2], r[3]) for r in rows if r[1] == "EUR"}
    assert D("2026-08-20") not in eur  # before the first rate: nothing, not the Friday rate
    assert eur[D("2026-08-22")] == (0.85, D("2026-08-21"))  # Saturday uses Friday
    assert eur[D("2026-08-23")] == (0.85, D("2026-08-21"))  # Sunday uses Friday
    assert eur[D("2026-08-24")] == (0.86, D("2026-08-24"))
    assert all(r[2] == 1.0 for r in rows if r[1] == "USD")


def test_usd_conversion(spark):
    fx = spark.createDataFrame(
        [(D("2026-09-05"), "EUR", 0.8, D("2026-09-04")), (D("2026-09-05"), "USD", 1.0, D("2026-09-05"))],
        "rate_date date, currency string, units_per_usd double, source_rate_date date")
    orders = spark.createDataFrame(
        [("a", D("2026-09-05"), 100.0, "EUR"), ("b", D("2026-09-05"), 50.0, "USD"),
         ("c", D("2026-09-05"), 10.0, "SAR"), ("d", D("2026-09-05"), None, "EUR"),
         ("e", D("2026-09-05"), 10.0, None)],
        "id string, decision_date date, order_amount double, order_currency string")
    got = {r.id: r.order_amount_usd for r in add_usd_amount(orders, fx).collect()}
    assert got == {"a": 125.0, "b": 50.0, "c": None, "d": None, "e": None}


def test_holidays_from_package():
    rows, unsupported = holiday_rows(["US", "XX", None], [2026])
    assert ("US", D("2026-09-07"), "Labor Day") in rows
    assert unsupported == ["XX"]


@pytest.fixture
def daily(spark):
    # publisher p: 10 impressions + 1 conversion arriving every day 09-01..09-10, and on 09-10 a
    # burst of 100 conversions that must NOT be visible to a request on 09-10.
    rows = [("p", D(f"2026-09-{d:02d}"), 10, 2, 1) for d in range(1, 10)]
    rows.append(("p", D("2026-09-10"), 10, 2, 100))
    return spark.createDataFrame(rows, "publisher_id string, arrival_date date, impressions long, clicks long, conversions long")


def test_history_uses_only_previous_days(spark, daily):
    feats = {r.feature_date: r for r in rolling_rates(daily, "publisher_id", windows=(7, 28)).collect()}
    on_10th = feats[D("2026-09-10")]
    assert on_10th.publisher_impressions_7d == 70  # 09-03..09-09, not 09-10
    assert on_10th.publisher_cvr_7d == pytest.approx(7 / 70)  # the burst on 09-10 is not in it
    assert on_10th.publisher_ctr_7d == pytest.approx(14 / 70)
    assert on_10th.publisher_impressions_28d == 90  # everything before 09-10
    first = feats[D("2026-09-01")]
    assert first.publisher_impressions_7d == 0 and first.publisher_cvr_7d is None  # no history yet
    after = feats[D("2026-09-11")]
    assert after.publisher_cvr_7d == pytest.approx((6 + 100) / 70)  # the burst is usable the next day


def test_fx_uses_saved_response_when_api_is_unreachable(tmp_path):
    import requests

    from falcon_dwh.config import load_config
    from falcon_dwh.external import get_fx_response, save_fx_response

    cfg = load_config()
    cache = tmp_path / "fx.json"
    cfg["external"]["fx_cache_path"] = str(cache)
    body = {"base": "USD", "rates": {"2026-08-24": {"EUR": 0.86}}}

    def down(*_args):
        raise requests.ConnectionError("no route to host")

    with pytest.raises(requests.ConnectionError):  # no saved response: fail loudly, no silent nulls
        get_fx_response(cfg, fetch=down)

    from falcon_dwh.external import fx_range

    start, end = fx_range(cfg)
    save_fx_response(cache, cfg["external"]["fx_url"], "USD", start, end, body)
    got, source = get_fx_response(cfg, fetch=down)
    assert got == body and "saved response" in source

    live, source = get_fx_response(cfg, fetch=lambda *_a: {"base": "USD", "rates": {}})
    assert live == {"base": "USD", "rates": {}} and source == cfg["external"]["fx_url"]
