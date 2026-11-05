"""Fact tables.

fact_request     grain: one row per request_id                 partition: request_date
Every fact has <event>_date -> dim_date.date and <event>_time_key -> dim_time.time_key (UTC).
fact_impression  grain: one row per shown slot (fid)           partition: impression_date
                 (date of the first render), with click and conversion outcomes
fact_postback    grain: one row per postback_id (deduplicated)  partition: postback_date
rejected_impressions  grain: one staged impression row that cannot be used (bad fid / time)

Incremental rule (late events): a run for day D rebuilds every fact partition that day D's
folder can change, from ALL staged data:
- requests in folder D        -> their request dates
- impressions in folder D     -> the impression dates of their fids
- clicks / postbacks in D     -> the impression dates of the fids they point to (can be weeks back)
- requests in folder D        -> impressions of those requests (a late request flips request_found)
Partitions are replaced, never appended to, so re-running any day gives the same tables.
Events are dated by their own created_at, not by the folder they arrived in.
"""
from pyspark.sql import Window
from pyspark.sql import functions as F

from falcon_dwh.config import table_name
from falcon_dwh.io import overwrite_partitions
from falcon_dwh.schemas import CORRUPT
from falcon_dwh.transforms import decode_fid, time_key, utc_ts

OPEN_END = "9999-12-31"


def _stg(spark, cfg, name):
    return spark.table(table_name(cfg, f"stg_{name}"))


def _dates(df, col):
    """Distinct dates of a small result (at most a few dozen values), safe to collect."""
    return [r[0] for r in df.select(col).where(F.col(col).isNotNull()).distinct().collect()]


# --------------------------------------------------------------------------- requests
def requests_clean(spark, cfg):
    """All staged requests, one row per request_id.

    Truncated JSON lines (in _corrupt_record) still carry request_id, placement_id and created_at
    at the start of the line; these are recovered by regex and flagged is_truncated_record.
    Duplicate request_ids keep the first arrival.
    """
    r = _stg(spark, cfg, "requests")
    raw = F.col(CORRUPT)

    def salvage(field):
        return F.nullif(F.regexp_extract(raw, f'"{field}":"([^"]*)"', 1), F.lit(""))

    r = r.select(
        F.coalesce("request_id", salvage("request_id")).alias("request_id"),
        F.coalesce("placement_id", salvage("placement_id")).alias("placement_id"),
        F.coalesce("created_at", F.to_timestamp(salvage("created_at"))).alias("request_ts"),
        "customer_id", "session_id", "user_agent", "city", "state", "country", "has_preview", "data",
        raw.isNotNull().alias("is_truncated_record"),
        "load_date",
    )
    w = Window.partitionBy("request_id").orderBy("load_date", "request_ts")
    r = r.withColumn("_rn", F.row_number().over(w)).withColumn("delivery_count", F.count("*").over(Window.partitionBy("request_id")))
    return (
        r.where("_rn = 1").drop("_rn")
        .withColumn("request_date", F.to_date("request_ts"))
        .withColumn("request_time_key", time_key(F.col("request_ts")))
    )


def parse_request_data(data):
    """Columns parsed out of the raw requests.data JSON text."""
    od = "$.orderData."
    g = lambda path: F.get_json_object(data, path)  # noqa: E731
    order_id = F.coalesce(g(od + "orderid"), g(od + "orderId"), g(od + "orderID"))
    amount_raw = F.coalesce(g(od + "amount"), g(od + "order_value"))
    return [
        order_id.alias("order_id"),
        amount_raw.alias("order_amount_raw"),  # kept as sent; order_amount is the parsed number
        F.upper(g(od + "currency")).alias("order_currency"),
        g(od + "country").alias("order_country"),
        g(od + "provinceCode").alias("order_province"),
        g(od + "paymenttype").alias("payment_type"),
        g(od + "language").alias("order_language"),
        g(od + "shopdomain").alias("shop_domain"),
        F.when(g(od + "cartItems").isNotNull(), F.size(F.from_json(g(od + "cartItems"), "array<string>")))
        .alias("cart_item_count"),
        g("$.hasPartnerOffer").cast("boolean").alias("has_partner_offer"),
        g("$.offerCount").cast("int").alias("offer_count"),
        g("$.isGdprUser").cast("boolean").alias("is_gdpr_user"),
        g("$.template").alias("template"),
        g("$.pubSub1").alias("pub_sub1"),
        g("$.pubSub2").alias("pub_sub2"),
    ]


def _as_of(df, dim, key, date_col, keep):
    """Left join a versioned dimension as of the event date. Never drops fact rows."""
    d = dim.select(F.col(key).alias("_k"), "valid_from", "valid_to", *keep)
    cond = (F.col(key) == F.col("_k")) & F.col(date_col).between(F.col("valid_from"), F.col("valid_to"))
    return df.join(F.broadcast(d), cond, "left").drop("_k", "valid_from", "valid_to")


def build_fact_request(spark, cfg, load_date):
    new = _stg(spark, cfg, "requests").where(F.col("load_date") == F.lit(load_date).cast("date"))
    salvaged_ts = F.to_timestamp(F.regexp_extract(F.col(CORRUPT), '"created_at":"([^"]*)"', 1))
    dates = _dates(new.select(F.to_date(F.coalesce("created_at", salvaged_ts)).alias("d")), "d")

    r = requests_clean(spark, cfg).where(F.col("request_date").isin(dates))
    r = (
        r.select("*", *parse_request_data(F.col("data"))).drop("data")
        # try_cast: a non-numeric amount becomes null instead of failing the job (counted in DQ)
        .withColumn("order_amount", F.expr("try_cast(trim(order_amount_raw) as double)"))
    )
    dim = spark.table(table_name(cfg, "dim_placement"))
    r = _as_of(r, dim, "placement_id", "request_date",
               ["placement_key", "site_id", "publisher_id", "is_test_mode"])
    r = r.withColumn("placement_found", F.col("placement_key").isNotNull()).withColumnRenamed(
        "load_date", "first_load_date")
    overwrite_partitions(r, cfg, "fact_request", "request_date", dates)
    return dates


# --------------------------------------------------------------------------- impressions
def impression_reject_reason():
    """Why a staged impression row cannot become a fact row (null = it can)."""
    req_id, _ = decode_fid(F.col("fid"))
    return (
        F.when(F.col("fid").isNull(), "fid_null")
        .when(req_id.isNull(), "fid_undecodable")
        .when(F.col("created_at").isNull(), "created_at_null")
    )


def _upto(df, upto):
    """Only folders loaded up to and including `upto` (None = everything staged)."""
    return df if upto is None else df.where(F.col("load_date") <= F.lit(upto).cast("date"))


def valid_impressions(spark, cfg, upto=None):
    return _upto(_stg(spark, cfg, "impressions"), upto).where(impression_reject_reason().isNull())


def build_rejected_impressions(spark, cfg, load_date):
    """Staged impression rows that cannot be placed in fact_impression, with the reason.
    Grain: one staged row. Partition: load_date (same as staging, for the reconciliation)."""
    day = F.lit(load_date).cast("date")
    rej = (
        _stg(spark, cfg, "impressions").where(F.col("load_date") == day)
        .withColumn("reject_reason", impression_reject_reason())
        .where(F.col("reject_reason").isNotNull())
    )
    overwrite_partitions(rej, cfg, "rejected_impressions", "load_date", [load_date])
    return [load_date]


def fid_index(spark, cfg, upto=None):
    """fid -> first render time / impression_date, from all valid staged impressions."""
    return (
        valid_impressions(spark, cfg, upto)
        .groupBy("fid")
        .agg(F.min(utc_ts(F.col("created_at"))).alias("first_render_ts"))
        .withColumn("impression_date", F.to_date("first_render_ts"))
    )


def affected_impression_dates(spark, cfg, load_date):
    day = F.lit(load_date).cast("date")
    touched = (
        _stg(spark, cfg, "impressions").where(F.col("load_date") == day).select("fid")
        .union(_stg(spark, cfg, "clicks").where(F.col("load_date") == day).select("fid"))
        .union(_stg(spark, cfg, "postbacks").where(F.col("load_date") == day).select("fid"))
    )
    new_requests = _stg(spark, cfg, "requests").where(F.col("load_date") == day).select("request_id")
    idx = fid_index(spark, cfg)
    req_id, _ = decode_fid(F.col("fid"))
    by_request = idx.withColumn("request_id", req_id).join(new_requests, "request_id", "left_semi").select("fid")
    hit = idx.join(touched.union(by_request).distinct(), "fid", "left_semi")
    return _dates(hit, "impression_date")


def clicks_by_fid(spark, cfg):
    c = _stg(spark, cfg, "clicks").withColumn("ts", utc_ts(F.col("created_at")))
    return c.groupBy("fid").agg(
        F.count("*").alias("click_count"),
        F.countDistinct("click_id").alias("click_id_count"),
        F.min("ts").alias("first_click_ts"),
    )


def postbacks_dedup(spark, cfg, upto=None):
    """One row per postback_id: 50 ids were delivered twice (identical, next day's folder)."""
    p = _upto(_stg(spark, cfg, "postbacks"), upto).withColumn("postback_ts", utc_ts(F.col("created_at")))
    w = Window.partitionBy("postback_id").orderBy("load_date")
    return (
        p.withColumn("_rn", F.row_number().over(w))
        .withColumn("delivery_count", F.count("*").over(Window.partitionBy("postback_id")))
        .where("_rn = 1").drop("_rn", "created_at")
        .withColumnRenamed("load_date", "first_load_date")
    )


def build_fact_impression(spark, cfg, load_date):
    dates = affected_impression_dates(spark, cfg, load_date)
    if not dates:
        return dates
    idx = fid_index(spark, cfg).where(F.col("impression_date").isin(dates))

    imp = (
        valid_impressions(spark, cfg).join(idx.select("fid"), "fid", "left_semi")
        .withColumn("ts", utc_ts(F.col("created_at")))
    )
    # several rows per fid = re-renders of the same slot; keep the first render, count the rest
    w = Window.partitionBy("fid").orderBy("ts", "impression_id")
    first = (
        imp.withColumn("_rn", F.row_number().over(w))
        .withColumn("render_count", F.count("*").over(Window.partitionBy("fid")))
        .withColumn("last_render_ts", F.max("ts").over(Window.partitionBy("fid")))
        .withColumn("first_load_date", F.min("load_date").over(Window.partitionBy("fid")))
        .where("_rn = 1")
    )
    req_id, position = decode_fid(F.col("fid"))
    f = first.select(
        "fid",
        req_id.alias("request_id"),
        position.alias("slot_position"),
        "impression_id",
        F.col("ts").alias("impression_ts"),
        F.to_date("ts").alias("impression_date"),
        time_key(F.col("ts")).alias("impression_time_key"),
        "placement_id", "banner_id", "status", "render_count", "last_render_ts", "first_load_date",
    )

    req = spark.table(table_name(cfg, "fact_request")).select(
        "request_id", "request_ts", F.col("placement_id").alias("request_placement_id"),
        "has_preview", F.lit(True).alias("request_found"))
    f = f.join(req, "request_id", "left").fillna(False, ["request_found"])
    f = f.withColumn(
        "placement_matches_request",
        F.when(F.col("request_found"), F.col("placement_id") == F.col("request_placement_id")))

    f = _as_of(f, spark.table(table_name(cfg, "dim_placement")), "placement_id", "impression_date",
               ["placement_key", "site_id", "publisher_id"])
    f = _as_of(f, spark.table(table_name(cfg, "dim_banner")), "banner_id", "impression_date",
               ["banner_key", "campaign_id", "advertiser_id"])

    clicks = clicks_by_fid(spark, cfg)
    pbs = (
        postbacks_dedup(spark, cfg).groupBy("fid").agg(
            F.count("*").alias("conversion_count"),
            F.sum("revenue").alias("revenue_usd"),
            F.min("postback_ts").alias("first_conversion_ts"),
        )
    )
    f = (
        f.join(clicks, "fid", "left").join(pbs, "fid", "left")
        .fillna(0, ["click_count", "click_id_count", "conversion_count"])
        .fillna(0.0, ["revenue_usd"])
        .withColumn("is_click", F.col("click_count") > 0)
        .withColumn("is_conversion", F.col("conversion_count") > 0)
        .withColumn("placement_found", F.col("placement_key").isNotNull())
        .withColumn("banner_found", F.col("banner_key").isNotNull())
    )
    overwrite_partitions(f, cfg, "fact_impression", "impression_date", dates)
    return dates


# --------------------------------------------------------------------------- postbacks
def build_fact_postback(spark, cfg, load_date):
    """Every conversion, also the ones whose fid matches no impression, so revenue is never lost."""
    new = _stg(spark, cfg, "postbacks").where(F.col("load_date") == F.lit(load_date).cast("date"))
    dates = _dates(new.select(F.to_date(utc_ts(F.col("created_at"))).alias("d")), "d")
    p = (
        postbacks_dedup(spark, cfg)
        .withColumn("postback_date", F.to_date("postback_ts"))
        .withColumn("postback_time_key", time_key(F.col("postback_ts")))
        .where(F.col("postback_date").isin(dates))
    )
    req_id, position = decode_fid(F.col("fid"))
    idx = fid_index(spark, cfg).select("fid", "first_render_ts", F.lit(True).alias("impression_found"))
    clicks = clicks_by_fid(spark, cfg).select("fid", "first_click_ts")
    p = (
        p.withColumn("request_id", req_id).withColumn("slot_position", position)
        .join(idx, "fid", "left").join(clicks, "fid", "left")
        .fillna(False, ["impression_found"])
        .withColumn("click_before_postback", F.coalesce(F.col("first_click_ts") <= F.col("postback_ts"), F.lit(False)))
        .withColumn("days_from_impression", F.datediff("postback_ts", "first_render_ts"))
    )
    overwrite_partitions(p, cfg, "fact_postback", "postback_date", dates)
    return dates


def build_facts(spark, cfg, load_date):
    return {
        "fact_request": build_fact_request(spark, cfg, load_date),
        "fact_impression": build_fact_impression(spark, cfg, load_date),
        "fact_postback": build_fact_postback(spark, cfg, load_date),
        "rejected_impressions": build_rejected_impressions(spark, cfg, load_date),
    }
