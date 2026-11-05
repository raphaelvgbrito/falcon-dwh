"""Unit tests for the de-duplication rules of the facts (the same functions the pipeline calls).

Timestamps are built on the server from strings, so the laptop's time zone cannot shift them.
"""
from pyspark.sql import functions as F

from falcon_dwh.facts import aggregate_clicks, clean_requests, dedup_postbacks, first_render_per_fid


def _utc_str(df, *cols):
    """Format timestamps on the server (session time zone = UTC) before collecting them."""
    for c in cols:
        df = df.withColumn(c, F.date_format(c, "yyyy-MM-dd HH:mm:ss"))
    return df


def _df(spark, rows, ddl, ts_cols=(), ntz=False):
    df = spark.createDataFrame(rows, ddl)
    for c in ts_cols:
        df = df.withColumn(c, F.col(c).cast("timestamp_ntz" if ntz else "timestamp"))
    return df.withColumn("load_date", F.col("load_date").cast("date"))


def test_rerenders_collapse_to_the_first_render(spark):
    rows = [
        # fid A: rendered 3 times over two folders, delivered out of order
        ("i2", "A", "2026-09-01 10:00:05", "2026-09-01"),
        ("i1", "A", "2026-09-01 10:00:01", "2026-09-01"),
        ("i3", "A", "2026-09-02 08:00:00", "2026-09-02"),
        # fid B: shown once
        ("i4", "B", "2026-09-01 11:00:00", "2026-09-01"),
        # fid C: two renders at the same instant -> lowest impression_id, deterministic
        ("i9", "C", "2026-09-01 12:00:00", "2026-09-01"),
        ("i6", "C", "2026-09-01 12:00:00", "2026-09-01"),
    ]
    df = _df(spark, rows, "impression_id string, fid string, created_at string, load_date string",
             ["created_at"], ntz=True)
    out = {r.fid: r for r in _utc_str(first_render_per_fid(df), "ts", "last_render_ts").collect()}

    assert sorted(out) == ["A", "B", "C"]  # one row per fid
    assert sum(r.render_count for r in out.values()) == len(rows)  # nothing lost: reconciles
    a = out["A"]
    assert (a.impression_id, a.render_count, str(a.first_load_date)) == ("i1", 3, "2026-09-01")
    assert str(a.ts) == "2026-09-01 10:00:01" and str(a.last_render_ts) == "2026-09-02 08:00:00"
    assert out["C"].impression_id == "i6"


def test_postbacks_delivered_twice_count_once(spark):
    rows = [
        ("p1", "F1", "2026-10-15 09:00:00", 10.0, "2026-10-15"),
        ("p1", "F1", "2026-10-15 09:00:00", 10.0, "2026-10-16"),  # same postback, next folder
        ("p2", "F2", "2026-10-15 10:00:00", 5.0, "2026-10-15"),
    ]
    df = _df(spark, rows, "postback_id string, fid string, created_at string, revenue double, load_date string",
             ["created_at"], ntz=True)
    out = {r.postback_id: r for r in dedup_postbacks(df).collect()}

    assert sorted(out) == ["p1", "p2"]
    assert (out["p1"].delivery_count, str(out["p1"].first_load_date)) == (2, "2026-10-15")
    assert sum(r.revenue for r in out.values()) == 15.0  # not 25


def test_repeated_clicks_make_one_clicked_slot(spark):
    rows = [
        ("c1", "A", "2026-09-01 10:00:00", "2026-09-01"),
        ("c2", "A", "2026-09-01 10:00:00", "2026-09-01"),  # same-second double click
        ("c3", "A", "2026-09-03 09:00:00", "2026-09-03"),
        ("c4", "B", "2026-09-01 11:00:00", "2026-09-01"),
    ]
    df = _df(spark, rows, "click_id string, fid string, created_at string, load_date string",
             ["created_at"], ntz=True)
    out = {r.fid: r for r in _utc_str(aggregate_clicks(df), "first_click_ts").collect()}

    assert sorted(out) == ["A", "B"]
    assert (out["A"].click_count, out["A"].click_id_count) == (3, 3)
    assert str(out["A"].first_click_ts) == "2026-09-01 10:00:00"
    assert out["B"].click_count == 1


def test_requests_dedup_and_truncated_record_recovery(spark):
    truncated = ('{"request_id":"22bc9cabeaaa00bd","placement_id":"d4cf7ed7991c59be",'
                 '"created_at":"2026-09-25T00:44:34.903Z","customer_id":null,"data"')
    cols = ("request_id string, placement_id string, created_at string, customer_id string, session_id string, "
            "user_agent string, city string, state string, country string, has_preview boolean, data string, "
            "_corrupt_record string, load_date string")
    rows = [
        ("r1", "P", "2026-09-01 10:00:00", None, "s", "ua", None, None, "US", False, "{}", None, "2026-09-01"),
        ("r1", "P", "2026-09-01 10:00:00", None, "s", "ua", None, None, "US", False, "{}", None, "2026-09-02"),
        (None, None, None, None, None, None, None, None, None, None, None, truncated, "2026-09-25"),
    ]
    out = {r.request_id: r for r in clean_requests(_df(spark, rows, cols, ["created_at"])).collect()}

    assert sorted(out) == ["22bc9cabeaaa00bd", "r1"]
    assert (out["r1"].delivery_count, str(out["r1"].load_date)) == (2, "2026-09-01")  # first arrival kept
    t = out["22bc9cabeaaa00bd"]
    assert t.is_truncated_record and t.placement_id == "d4cf7ed7991c59be"
    assert str(t.request_date) == "2026-09-25"
    assert t.request_time_key == 44  # 00:44 UTC
