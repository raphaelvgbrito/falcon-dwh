"""Unit tests for SCD2 versioning of daily snapshots."""
import datetime as dt

from pyspark.sql import functions as F

from falcon_dwh.dims import OPEN_END, dedup_snapshot, dim_time_df, scd2

D = dt.date.fromisoformat


def _versions(spark, rows, snapshot_days):
    snaps = spark.createDataFrame([(k, D(d), v) for k, d, v in rows], "id string, load_date date, val string")
    dates = spark.createDataFrame([(D(d),) for d in snapshot_days], "load_date date")
    out = scd2(snaps, "id", ["val"], dates).orderBy("id", "valid_from").collect()
    return [(r.id, r.val, str(r.valid_from), str(r.valid_to), r.is_current) for r in out]


def test_unchanged_rows_collapse_and_change_opens_new_version(spark):
    days = ["2026-09-01", "2026-09-02", "2026-09-03"]
    rows = [("a", "2026-09-01", "x"), ("a", "2026-09-02", "x"), ("a", "2026-09-03", "y")]
    assert _versions(spark, rows, days) == [
        ("a", "x", "2026-09-01", "2026-09-02", False),
        ("a", "y", "2026-09-03", OPEN_END, True),
    ]


def test_missing_folder_does_not_split_a_version(spark):
    # no snapshot at all on 09-02 (like dt=2026-10-03): the version just continues
    days = ["2026-09-01", "2026-09-03"]
    rows = [("a", "2026-09-01", "x"), ("a", "2026-09-03", "x")]
    assert _versions(spark, rows, days) == [("a", "x", "2026-09-01", OPEN_END, True)]


def test_id_missing_from_a_snapshot_closes_its_version(spark):
    days = ["2026-09-01", "2026-09-02", "2026-09-03"]
    rows = [("a", "2026-09-01", "x"), ("a", "2026-09-03", "x"), ("b", "2026-09-01", "z")]
    assert _versions(spark, rows, days) == [
        ("a", "x", "2026-09-01", "2026-09-01", False),
        ("a", "x", "2026-09-03", OPEN_END, True),
        ("b", "z", "2026-09-01", "2026-09-01", False),  # deleted after 09-01
    ]


def test_dedup_snapshot_keeps_latest_updated_row(spark):
    df = spark.createDataFrame(
        [
            ("p1", D("2026-09-12"), "15", dt.datetime(2026, 10, 14, 18, 53, 19)),
            ("p1", D("2026-09-12"), "99", dt.datetime(2026, 9, 12, 9, 0, 0)),
            ("p2", D("2026-09-12"), "1", None),
        ],
        "id string, load_date date, template string, updated_at timestamp",
    )
    got = sorted((r.id, r.template) for r in dedup_snapshot(df).collect())
    assert got == [("p1", "15"), ("p2", "1")]


def test_dim_time_has_every_minute_once(spark):
    rows = {r.time_key: r for r in dim_time_df(spark).collect()}
    assert len(rows) == 1440
    assert (rows[0].time_label, rows[0].part_of_day, rows[0].am_pm) == ("00:00", "night", "AM")
    r = rows[1435]
    assert (r.hour, r.minute, r.minute_of_day, r.hour_label, r.part_of_day, r.am_pm) == (
        14, 35, 875, "14:00-14:59", "afternoon", "PM")
    assert rows[2359].time_label == "23:59" and 2360 not in rows


def test_time_key_matches_dim_time(spark):
    from falcon_dwh.transforms import time_key

    # Build timestamps on the server from strings: a Python datetime would be read in the
    # laptop's local zone by the client and shifted, while pipeline data is UTC from the files.
    df = spark.createDataFrame([("2026-09-01 14:35:59",), ("2026-09-01 00:00:00",)], "s string")
    ts = F.to_timestamp("s")
    assert [r[0] for r in df.select(time_key(ts)).collect()] == [1435, 0]
