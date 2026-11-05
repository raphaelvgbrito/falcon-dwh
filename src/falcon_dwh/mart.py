"""ML training mart.

ml_training   grain: one row per shown slot (fid), like fact_impression   partition: impression_date
    labels   : is_click, is_conversion (as known after everything loaded so far; see label_age_days)
    features : known at request time only
        request context, order amount in USD, device / OS / browser from user_agent,
        placement + banner attributes (dimension version valid on the impression date),
        site + campaign enrichment (newest fetch with created_at <= request time),
        time features, public-holiday flag, history features (arrivals before the request day)

Decision time = the request time (impression time when the request is unknown).
Caveat from the spec: dimension and enrichment snapshots carry today's attribute values, so this
table does not support a leakage-free historical backtest. Event-based features (history) do
respect real arrival times.
Preview-tool traffic is kept with is_preview=true so a training job can filter it.
"""
from pyspark.sql import Window
from pyspark.sql import functions as F

from falcon_dwh.config import table_name
from falcon_dwh.history import history_features
from falcon_dwh.io import overwrite_partitions
from falcon_dwh.schemas import CORRUPT
from falcon_dwh.transforms import parse_user_agent

ENRICH_ATTRS = [
    "main_category", "subcategory", "desktop_traffic_share", "mobile_traffic_share", "male_share",
    "female_share", "age_18_to_24", "age_25_to_34", "age_35_to_44", "age_45_to_54", "age_55_to_64",
    "age_65_plus", "top_country",
]


def enrichment_rows(spark, cfg, name, key):
    """Every enrichment fetch ever seen (one row per fetch id, latest snapshot of it)."""
    df = spark.table(table_name(cfg, f"stg_{name}")).where(F.col(CORRUPT).isNull())
    w = Window.partitionBy("id").orderBy(F.col("load_date").desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").select(key, "created_at", *ENRICH_ATTRS)


def as_of_enrichment(df, enr, key, ts_col, prefix):
    """Newest enrichment row with created_at <= decision time, per fid."""
    e = enr.select(F.col(key).alias("_ek"), F.col("created_at").alias("_ets"),
                   *[F.col(c).alias(f"{prefix}{c}") for c in ENRICH_ATTRS])
    j = df.select("fid", key, ts_col).join(
        e, (F.col(key) == F.col("_ek")) & (F.col("_ets") <= F.col(ts_col)), "inner")
    w = Window.partitionBy("fid").orderBy(F.col("_ets").desc())
    best = j.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").select(
        "fid", F.col("_ets").alias(f"{prefix}fetched_at"), *[f"{prefix}{c}" for c in ENRICH_ATTRS])
    return df.join(best, "fid", "left")


def add_usd_amount(df, fx_rates):
    """order_amount_usd = order_amount / units_per_usd on the decision day.

    ref_fx_rates has a row for every calendar day, filled forward from the last ECB rate on or
    before that day, so weekends work and no later rate is used. Unknown currency, no currency or
    no amount -> null (counted in the DQ report), never a guess.
    """
    fx = fx_rates.select(
        F.col("rate_date").alias("decision_date"), F.col("currency").alias("order_currency"), "units_per_usd")
    return df.join(F.broadcast(fx), ["decision_date", "order_currency"], "left").withColumn(
        "order_amount_usd", F.round(F.col("order_amount") / F.col("units_per_usd"), 2)).drop("units_per_usd")


def build_ml_mart(spark, cfg, dates):
    if not dates:
        return dates
    T = lambda n: spark.table(table_name(cfg, n))  # noqa: E731

    fi = T("fact_impression").where(F.col("impression_date").isin(dates)).select(
        "fid", "impression_date", "impression_ts", "request_id", "slot_position", "placement_id",
        "banner_id", "placement_key", "banner_key", "site_id", "publisher_id", "campaign_id",
        "advertiser_id", "request_found", "is_click", "is_conversion")

    fr = T("fact_request").select(
        "request_id", "request_ts", "country", "state", "city", "customer_id", "user_agent",
        F.col("has_preview").alias("is_preview"), "order_amount", "order_currency", "order_language",
        "has_partner_offer", "offer_count", "is_gdpr_user", F.col("template").alias("request_template"),
        "cart_item_count")
    m = fi.join(fr, "request_id", "left")
    m = m.withColumn("decision_ts", F.coalesce("request_ts", "impression_ts")).withColumn(
        "decision_date", F.to_date("decision_ts"))

    # request context
    device, os, os_major, browser = parse_user_agent(F.col("user_agent"))
    m = (
        m.withColumn("device_type", device).withColumn("os", os).withColumn("os_major", os_major)
        .withColumn("browser", browser)
        .withColumn("has_customer_id", F.col("customer_id").isNotNull()).drop("customer_id", "user_agent")
        .withColumn("hour_utc", F.hour("decision_ts"))
        .withColumn("day_of_week", F.dayofweek("decision_date"))  # 1 = Sunday
        .withColumn("is_weekend", F.dayofweek("decision_date").isin(1, 7))
    )

    m = add_usd_amount(m, T("ref_fx_rates"))

    # holiday in the shopper's country (from the IP) on the request day (UTC date)
    hol = T("ref_holidays").select(
        F.col("country_code").alias("country"), F.col("holiday_date").alias("decision_date"), "holiday_name")
    m = m.join(F.broadcast(hol), ["country", "decision_date"], "left").withColumn(
        "is_public_holiday", F.col("holiday_name").isNotNull())

    # placement + banner attributes: the dimension version valid on the impression date
    dp = T("dim_placement").select(
        "placement_key", "placement_type", F.col("template").alias("placement_template"), "surface_type",
        "is_test_mode", "user_initiated", "site_category_name", "site_order_volume", "site_average_order",
        "publisher_rev_share_percentage", "platform_name")
    db = T("dim_banner").select(
        "banner_key", "banner_type", "banner_weight", "campaign_priority", "campaign_weight", "campaign_cpa",
        "campaign_cpa_source", "campaign_category_name", "advertiser_payout")
    m = m.join(F.broadcast(dp), "placement_key", "left").join(F.broadcast(db), "banner_key", "left")

    # enrichment as of the decision time
    m = as_of_enrichment(m, enrichment_rows(spark, cfg, "site_enrichment", "site_id"), "site_id", "decision_ts", "site_enr_")
    m = as_of_enrichment(m, enrichment_rows(spark, cfg, "campaign_enrichment", "campaign_id"), "campaign_id",
                         "decision_ts", "campaign_enr_")

    # history: only arrivals before the decision day
    pub, camp = history_features(spark, cfg)
    m = (
        m.join(pub.withColumnRenamed("feature_date", "decision_date"), ["publisher_id", "decision_date"], "left")
        .join(camp.withColumnRenamed("feature_date", "decision_date"), ["campaign_id", "decision_date"], "left")
    )

    # No "label age" column on purpose: it depends on the run date, so a rebuilt partition would
    # differ from an untouched one. Consumers compute it as (as-of date - impression_date).
    overwrite_partitions(m, cfg, "ml_training", "impression_date", dates)
    return dates
