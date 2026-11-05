"""Data quality report and impression reconciliation, per run day.

dq_report             grain: one row per (load_date, check, table)    partition: load_date
    check, table_name, total_rows, failed_rows, example (one failing row as JSON), action
recon_impressions     grain: one row per load_date                    partition: load_date
    stg_rows = fact_rows + collapsed_rerenders + rejected_rows, and fact_rows found in the fact

Checks look at what arrived in folder D (staging partition D) and at the fact rows first loaded
from it. Look-ups into other days (duplicates, click totals, does a click have an impression)
only use folders up to D, so the report of day D means "as known on day D" and re-running D,
even after later days were loaded, gives the same staging-based results. Checks on fact flags
show the fact as it is at run time (a late request can still flip request_found later).
Examples use min() over the failing rows, so a re-run produces the identical report.
"""
from pyspark.sql import Window
from pyspark.sql import functions as F

from falcon_dwh.config import table_name
from falcon_dwh.facts import _upto, fid_index, impression_reject_reason, postbacks_dedup, valid_impressions
from falcon_dwh.io import overwrite_partitions
from falcon_dwh.schemas import CORRUPT, SOURCES

CLICK_FLOOD = 100  # clicks on one slot above this are treated as non-human
LONG_LAG_DAYS = 30

KEY_COLUMNS = {
    "requests": ["request_id", "placement_id", "created_at"],
    "impressions": ["impression_id", "fid", "placement_id", "banner_id", "created_at"],
    "clicks": ["click_id", "fid", "created_at"],
    "postbacks": ["postback_id", "fid", "created_at", "revenue"],
    "orders": ["order_id", "site_id", "created_at", "amount", "currency"],
}
EVENTS = ["requests", "impressions", "clicks", "postbacks", "orders"]
SNAPSHOTS = [n for n, s in SOURCES.items() if s[3] == "snapshot"]


def _check(df, cond, check, table, action, example_cols=None):
    """One DQ row: how many rows of df match cond, one example, and what we did."""
    example_cols = example_cols or df.columns
    example = F.to_json(F.struct(*[F.col(c).cast("string").alias(c) for c in example_cols]))
    return df.agg(
        F.count("*").alias("total_rows"),
        F.coalesce(F.sum(cond.cast("int")), F.lit(0)).cast("bigint").alias("failed_rows"),
        F.min(F.when(cond, example)).alias("example"),
    ).select(
        F.lit(check).alias("check"), F.lit(table).alias("table_name"),
        "total_rows", "failed_rows", "example", F.lit(action).alias("action"),
    )


def _stg_day(spark, cfg, name, day):
    return spark.table(table_name(cfg, f"stg_{name}")).where(F.col("load_date") == day)


def manifest_counts(spark, cfg, load_date):
    """Expected rows per table for this day from manifest.json (read with Spark, no local file)."""
    path = f"{cfg['input']['base_path'].rstrip('/')}/manifest.json"
    m = spark.read.text(path, wholetext=True).select(
        F.from_json("value", "struct<rows: map<string, map<string, bigint>>>").alias("m"))
    return (
        m.select(F.explode("m.rows").alias("folder", "tables"))
        .where(F.col("folder") == f"dt={load_date}")
        .select(F.explode("tables").alias("table_name", "expected_rows"))
    )


def row_count_checks(spark, cfg, load_date, day):
    staged = None
    for name in SOURCES:
        c = _stg_day(spark, cfg, name, day).agg(F.count("*").alias("staged_rows")).select(
            F.lit(name).alias("table_name"), "staged_rows")
        staged = c if staged is None else staged.union(c)
    j = staged.join(manifest_counts(spark, cfg, load_date), "table_name", "full")
    diff = F.abs(F.coalesce("staged_rows", F.lit(0)) - F.coalesce("expected_rows", F.lit(0)))
    return j.select(
        F.lit("row_count_vs_manifest").alias("check"), "table_name",
        F.coalesce("staged_rows", F.lit(0)).cast("bigint").alias("total_rows"),
        diff.cast("bigint").alias("failed_rows"),
        F.concat(F.lit("expected="), F.coalesce(F.col("expected_rows").cast("string"), F.lit("missing")),
                 F.lit(" staged="), F.col("staged_rows").cast("string")).alias("example"),
        F.when(F.col("expected_rows").isNull(), "day folder missing from delivery: nothing loaded, reported")
        .otherwise("none if 0; otherwise investigate the load").alias("action"),
    )


def build_dq_report(spark, cfg, load_date):
    day = F.lit(load_date).cast("date")
    stg = {n: _stg_day(spark, cfg, n, day) for n in SOURCES}
    out = [row_count_checks(spark, cfg, load_date, day)]

    # corrupt (unparseable) rows
    for name, df in stg.items():
        if CORRUPT in df.columns:
            action = ("request_id / placement_id / created_at recovered by regex, flagged is_truncated_record"
                      if name == "requests" else "excluded from the warehouse, kept in staging")
            out.append(_check(df, F.col(CORRUPT).isNotNull(), "corrupt_record", name, action,
                              ["load_date", CORRUPT]))

    # nulls in key columns
    for name, cols in KEY_COLUMNS.items():
        for c in cols:
            out.append(_check(stg[name], F.col(c).isNull(), f"null_{c}", name,
                              "kept; downstream flag (e.g. placement_found=false, no USD amount)"))

    # late arrivals: event rows in folder D created on an earlier day
    for name in EVENTS:
        out.append(_check(stg[name], F.to_date("created_at") < day, "late_arrival", name,
                          "dated by created_at; earlier fact partitions rebuilt"))

    # duplicates
    all_req = _upto(spark.table(table_name(cfg, "stg_requests")), load_date)
    w = Window.partitionBy("request_id").orderBy("load_date", "created_at")
    dup_req = all_req.withColumn("_rn", F.row_number().over(w)).where(F.col("load_date") == day)
    out.append(_check(dup_req, F.col("_rn") > 1, "duplicate_key", "requests",
                      "kept first arrival only", ["request_id", "load_date", "created_at"]))

    all_pb = _upto(spark.table(table_name(cfg, "stg_postbacks")), load_date)
    w = Window.partitionBy("postback_id").orderBy("load_date")
    dup_pb = all_pb.withColumn("_rn", F.row_number().over(w)).where(F.col("load_date") == day)
    out.append(_check(dup_pb, F.col("_rn") > 1, "duplicate_key", "postbacks",
                      "kept first delivery; revenue counted once",
                      ["postback_id", "fid", "load_date", "created_at", "revenue"]))

    for name in SNAPSHOTS:
        df = stg[name].withColumn("_n", F.count("*").over(Window.partitionBy("id")))
        out.append(_check(df, F.col("_n") > 1, "duplicate_id_in_snapshot", name,
                          "kept the row with the latest updated_at", ["id", "load_date"]))

    # impressions: re-renders collapse into one fact row per fid
    chosen = chosen_impressions(spark, cfg, load_date).where(F.col("load_date") == day)
    out.append(_check(chosen, ~F.col("is_fact_row"), "rerender_collapsed", "impressions",
                      "one fact row per fid (first render), render_count kept",
                      ["fid", "impression_id", "created_at"]))
    out.append(_check(stg["impressions"], impression_reject_reason().isNotNull(), "rejected", "impressions",
                      "written to rejected_impressions with reject_reason", ["impression_id", "fid"]))

    # clicks / postbacks without an impression
    idx = fid_index(spark, cfg, load_date).select("fid", F.lit(True).alias("_has_imp"))
    clk = stg["clicks"].join(idx, "fid", "left")
    out.append(_check(clk, F.col("_has_imp").isNull(), "orphan_no_impression", "clicks",
                      "kept in staging only; cannot be attributed to a slot", ["click_id", "fid", "created_at"]))
    pb = stg["postbacks"].join(idx, "fid", "left")
    out.append(_check(pb, F.col("_has_imp").isNull(), "orphan_no_impression", "postbacks",
                      "kept in fact_postback with impression_found=false (revenue not lost)",
                      ["postback_id", "fid", "created_at", "revenue"]))

    # click floods: slots with more clicks than a human makes
    per_fid = _upto(spark.table(table_name(cfg, "stg_clicks")), load_date).groupBy("fid").agg(F.count("*").alias("_fid_clicks"))
    flood = stg["clicks"].join(per_fid, "fid")
    out.append(_check(flood, F.col("_fid_clicks") > CLICK_FLOOD, "click_flood", "clicks",
                      f"kept; is_click counts once per slot; slot has > {CLICK_FLOOD} clicks in total, review for fraud",
                      ["fid", "_fid_clicks"]))

    # postbacks: suspicious values
    pbd = postbacks_dedup(spark, cfg, load_date).where(F.col("first_load_date") == day).join(
        fid_index(spark, cfg, load_date).select("fid", "first_render_ts"), "fid", "left")
    out.append(_check(pbd, F.datediff("postback_ts", "first_render_ts") > LONG_LAG_DAYS, "long_lag", "postbacks",
                      f"kept; conversion > {LONG_LAG_DAYS} days after the impression, review attribution",
                      ["postback_id", "fid", "first_render_ts", "postback_ts", "revenue"]))
    out.append(_check(pbd, F.col("revenue") == 0, "zero_revenue", "postbacks",
                      "kept; counts as conversion, adds 0 revenue", ["postback_id", "fid", "revenue"]))

    # facts first loaded from this folder vs dimensions / requests
    fi = spark.table(table_name(cfg, "fact_impression")).where(F.col("first_load_date") == day)
    ex = ["fid", "request_id", "placement_id", "banner_id", "impression_date"]
    out.append(_check(fi, ~F.col("placement_found"), "no_dimension_match_placement", "fact_impression",
                      "kept with placement_found=false", ex))
    out.append(_check(fi, ~F.col("banner_found"), "no_dimension_match_banner", "fact_impression",
                      "kept with banner_found=false", ex))
    out.append(_check(fi, ~F.col("request_found"), "no_matching_request", "fact_impression",
                      "kept with request_found=false; no request features in the ML mart", ex))
    out.append(_check(fi, F.col("placement_matches_request") == False, "placement_differs_from_request",  # noqa: E712
                      "fact_impression", "kept; impression placement used for the placement dimension",
                      ex + ["request_placement_id"]))

    fr = spark.table(table_name(cfg, "fact_request")).where(F.col("first_load_date") == day)
    exr = ["request_id", "placement_id", "order_amount_raw", "order_currency"]
    out.append(_check(fr, ~F.col("placement_found"), "no_dimension_match_placement", "fact_request",
                      "kept with placement_found=false", exr))
    out.append(_check(fr, F.col("order_amount_raw").isNotNull() & F.col("order_amount").isNull(),
                      "order_amount_not_numeric", "fact_request", "order_amount=null, raw text kept", exr))
    out.append(_check(fr, F.col("order_amount").isNotNull() & F.col("order_currency").isNull(),
                      "order_amount_without_currency", "fact_request", "no USD amount possible", exr))
    out.append(_check(fr, F.col("has_preview") == True, "preview_traffic", "fact_request",  # noqa: E712
                      "kept, flagged has_preview / is_preview in the ML mart so training can filter it", exr))
    fx_ccy = spark.table(table_name(cfg, "ref_fx_rates")).select(F.col("currency").alias("order_currency")).distinct()
    frc = fr.join(fx_ccy.withColumn("_has_rate", F.lit(True)), "order_currency", "left")
    out.append(_check(frc, F.col("order_amount").isNotNull() & F.col("order_currency").isNotNull() & F.col("_has_rate").isNull(),
                      "currency_without_fx_rate", "fact_request",
                      "order_amount_usd = null in the ML mart (ECB/frankfurter has no rate for it)", exr))

    report = out[0]
    for df in out[1:]:
        report = report.unionByName(df)
    report = report.withColumn("load_date", day)
    overwrite_partitions(report, cfg, "dq_report", "load_date", [load_date])


def chosen_impressions(spark, cfg, upto=None):
    """Every valid staged impression row, marked whether it is the row kept in fact_impression.
    Same ordering as facts.build_fact_impression (first render, then impression_id)."""
    w = Window.partitionBy("fid").orderBy(F.col("created_at"), "impression_id")
    return valid_impressions(spark, cfg, upto).withColumn("is_fact_row", F.row_number().over(w) == 1)


def build_recon_impressions(spark, cfg, load_date):
    """stg rows of folder D = fact rows + collapsed re-renders + rejected rows, and every
    'fact row' really exists in fact_impression."""
    day = F.lit(load_date).cast("date")
    stg_rows = _stg_day(spark, cfg, "impressions", day).agg(F.count("*").alias("stg_rows"))
    rejected = spark.table(table_name(cfg, "rejected_impressions")).where(F.col("load_date") == day).agg(
        F.count("*").alias("rejected_rows"))
    fact_ids = spark.table(table_name(cfg, "fact_impression")).select("impression_id", F.lit(1).alias("_in_fact"))
    chosen = chosen_impressions(spark, cfg, load_date).where(F.col("load_date") == day).join(
        fact_ids, "impression_id", "left")
    parts = chosen.agg(
        F.count_if("is_fact_row").alias("fact_rows"),
        F.count_if(~F.col("is_fact_row")).alias("collapsed_rerenders"),
        F.count_if(F.col("is_fact_row") & F.col("_in_fact").isNotNull()).alias("fact_rows_found_in_fact"),
    )
    r = stg_rows.crossJoin(parts).crossJoin(rejected).withColumn("load_date", day)
    r = r.withColumn("difference", F.col("stg_rows") - F.col("fact_rows") - F.col("collapsed_rerenders") - F.col("rejected_rows"))
    r = r.withColumn("reconciled", (F.col("difference") == 0) & (F.col("fact_rows") == F.col("fact_rows_found_in_fact")))
    overwrite_partitions(r, cfg, "recon_impressions", "load_date", [load_date])


def build_quality(spark, cfg, load_date):
    build_dq_report(spark, cfg, load_date)
    build_recon_impressions(spark, cfg, load_date)
