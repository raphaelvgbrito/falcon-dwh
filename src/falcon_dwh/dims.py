"""Dimensions, versioned (SCD type 2) from the daily snapshots.

Every day folder has a full copy of each dimension "as known that day". The rule from the spec:
the snapshot of day D is what we knew on D, and a later snapshot wins from its day on.

dim_placement  grain: one row per placement id per version   key: placement_key
               (placement -> site -> publisher -> platform, + site category)
dim_banner     grain: one row per banner id per version      key: banner_key
               (banner -> campaign -> advertiser, + campaign category)

A new version starts when any attribute in the flattened row changes (also a change in the parent
site / publisher / campaign / advertiser), or when the id comes back after missing from snapshots.
valid_from / valid_to are inclusive dates; the current version has valid_to = 9999-12-31.
Facts join on id + event date BETWEEN valid_from AND valid_to.

The dims are rebuilt from all staged snapshots on every run. They are tiny (<2k ids, 62
snapshots), so a full rebuild is cheaper than merge logic and trivially idempotent.
"""
from pyspark.sql import Window
from pyspark.sql import functions as F

from falcon_dwh.config import table_name
from falcon_dwh.io import overwrite_table
from falcon_dwh.schemas import CORRUPT
from falcon_dwh.transforms import tf_bool

OPEN_END = "9999-12-31"


def dedup_snapshot(df, key="id"):
    """One row per id per snapshot day. A snapshot should never repeat an id, but placement
    3d2c6824fe8d1a8a does (two rows, different template). Keep the most recently updated row;
    the row hash breaks ties so the choice is deterministic. Counted in the DQ report."""
    order = [F.col("updated_at").desc_nulls_last()] if "updated_at" in df.columns else []
    order.append(F.xxhash64(*[c for c in df.columns if c != "_source_file"]))
    w = Window.partitionBy(key, "load_date").orderBy(*order)
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def _snap(spark, cfg, name, columns):
    """Valid, de-duplicated rows of one staged snapshot table. columns: {source column: output name}."""
    df = dedup_snapshot(spark.table(table_name(cfg, f"stg_{name}")).where(F.col(CORRUPT).isNull()))
    return df.select("load_date", *[F.col(src).alias(dst) for src, dst in columns.items()])


def scd2(snapshots, key, attr_cols, snapshot_dates):
    """Collapse daily snapshots (key, load_date, attrs...) into versions.

    snapshot_dates: DataFrame(load_date) with every snapshot date that exists, so an id that is
    missing from one snapshot closes its version, while the missing *folder* 2026-10-03 does not.
    """
    dates = snapshot_dates.select(
        "load_date",
        F.row_number().over(Window.orderBy("load_date")).alias("snap_seq"),
        F.lead("load_date").over(Window.orderBy("load_date")).alias("next_snapshot_date"),
    )
    w = Window.partitionBy(key).orderBy("load_date")
    s = snapshots.join(dates, "load_date").withColumn("_h", F.xxhash64(*attr_cols))
    s = s.withColumn(
        "_new",
        (
            F.lag("_h").over(w).isNull()
            | (F.lag("_h").over(w) != F.col("_h"))
            | (F.lag("snap_seq").over(w) != F.col("snap_seq") - 1)
        ).cast("int"),
    ).withColumn("_version", F.sum("_new").over(w.rowsBetween(Window.unboundedPreceding, 0)))

    versions = s.groupBy(key, "_version").agg(
        F.min("load_date").alias("valid_from"),
        F.max("load_date").alias("last_seen_date"),
        # attributes are identical inside a version (that is what the hash checks); take the latest
        F.max_by(F.struct(*attr_cols), "load_date").alias("_a"),
        F.max_by("next_snapshot_date", "load_date").alias("_next_snap"),
    )
    valid_to = F.coalesce(F.date_sub("_next_snap", 1), F.lit(OPEN_END).cast("date"))
    return versions.select(
        F.xxhash64(key, "valid_from").alias("_sk"),
        key,
        "_a.*",
        "valid_from",
        valid_to.alias("valid_to"),
        "last_seen_date",
        (F.col("_next_snap").isNull()).alias("is_current"),
    )


def build_dim_placement(spark, cfg):
    placement = _snap(spark, cfg, "placement", {
        "id": "placement_id", "name": "placement_name", "type": "placement_type",
        "template": "template", "is_test_mode": "is_test_mode", "site_id": "site_id",
        "surface_type": "surface_type", "user_initiated": "user_initiated",
        "created_at": "placement_created_at"})
    site = _snap(spark, cfg, "site", {
        "id": "site_id", "name": "site_name", "domain": "site_domain", "status": "site_status",
        "ads_enabled": "site_ads_enabled", "category_id": "site_category_id",
        "publisher_id": "publisher_id", "order_volume": "site_order_volume",
        "average_order": "site_average_order"})
    publisher = _snap(spark, cfg, "publisher", {
        "id": "publisher_id", "name": "publisher_name", "status": "publisher_status",
        "ads_enabled": "publisher_ads_enabled", "rev_share_percentage": "publisher_rev_share_percentage",
        "platform_id": "platform_id"})
    platform = _snap(spark, cfg, "platform", {"id": "platform_id", "name": "platform_name"})
    category = _snap(spark, cfg, "category", {"id": "site_category_id", "name": "site_category_name"})

    # left joins inside the same snapshot day: a broken link is kept and flagged, not dropped
    flat = (
        placement.join(site.withColumn("site_found", F.lit(True)), ["site_id", "load_date"], "left")
        .join(publisher.withColumn("publisher_found", F.lit(True)), ["publisher_id", "load_date"], "left")
        .join(platform, ["platform_id", "load_date"], "left")
        .join(category, ["site_category_id", "load_date"], "left")
        .withColumn("is_test_mode", tf_bool(F.col("is_test_mode")))
        .withColumn("user_initiated", tf_bool(F.col("user_initiated")))
        .withColumn("site_ads_enabled", tf_bool(F.col("site_ads_enabled")))
        .withColumn("publisher_ads_enabled", tf_bool(F.col("publisher_ads_enabled")))
        .fillna(False, ["site_found", "publisher_found"])
    )
    attrs = [c for c in flat.columns if c not in ("placement_id", "load_date")]
    dates = spark.table(table_name(cfg, "stg_placement")).select("load_date").distinct()
    dim = scd2(flat, "placement_id", attrs, dates).withColumnRenamed("_sk", "placement_key")
    overwrite_table(dim, cfg, "dim_placement")


def build_dim_banner(spark, cfg):
    banner = _snap(spark, cfg, "banner", {
        "id": "banner_id", "name": "banner_name", "title": "banner_title", "weight": "banner_weight",
        "campaign_id": "campaign_id", "is_active": "banner_is_active", "banner_type": "banner_type",
        "created_at": "banner_created_at"})
    campaign = _snap(spark, cfg, "campaign", {
        "id": "campaign_id", "name": "campaign_name", "category_id": "campaign_category_id",
        "weight": "campaign_weight", "priority": "campaign_priority", "cpa": "campaign_cpa",
        "cpa_source": "campaign_cpa_source", "advertiser_id": "advertiser_id"})
    advertiser = _snap(spark, cfg, "advertiser", {
        "id": "advertiser_id", "name": "advertiser_name", "payout": "advertiser_payout"})
    category = _snap(spark, cfg, "category", {
        "id": "campaign_category_id", "name": "campaign_category_name"})

    flat = (
        banner.join(campaign.withColumn("campaign_found", F.lit(True)), ["campaign_id", "load_date"], "left")
        .join(advertiser.withColumn("advertiser_found", F.lit(True)), ["advertiser_id", "load_date"], "left")
        .join(category, ["campaign_category_id", "load_date"], "left")
        .withColumn("banner_is_active", tf_bool(F.col("banner_is_active")))
        .fillna(False, ["campaign_found", "advertiser_found"])
    )
    attrs = [c for c in flat.columns if c not in ("banner_id", "load_date")]
    dates = spark.table(table_name(cfg, "stg_banner")).select("load_date").distinct()
    dim = scd2(flat, "banner_id", attrs, dates).withColumnRenamed("_sk", "banner_key")
    overwrite_table(dim, cfg, "dim_banner")


def build_dim_date(spark, cfg):
    """dim_date   grain: one row per calendar day of the run range   key: date

    Facts join on their partition date (request_date, impression_date, postback_date).
    Public holidays are per country, so they live in ref_holidays (country_code, holiday_date).
    source_folder_loaded is false for days whose folder never arrived (2026-10-03).
    """
    run = cfg["run"]
    days = spark.range(1).select(
        F.explode(F.sequence(F.lit(run["first_date"]).cast("date"), F.lit(run["last_date"]).cast("date"))).alias("date"))
    loaded = spark.table(table_name(cfg, "stg_impressions")).select(F.col("load_date").alias("date")).distinct()
    dim = (
        days.join(loaded.withColumn("source_folder_loaded", F.lit(True)), "date", "left")
        .fillna(False, ["source_folder_loaded"])
        .select(
            "date",
            F.year("date").alias("year"),
            F.quarter("date").alias("quarter"),
            F.month("date").alias("month"),
            F.date_format("date", "MMMM").alias("month_name"),
            F.weekofyear("date").alias("iso_week"),
            F.dayofmonth("date").alias("day_of_month"),
            F.dayofweek("date").alias("day_of_week"),  # 1 = Sunday, same as the ML mart
            F.date_format("date", "EEEE").alias("day_name"),
            F.dayofweek("date").isin(1, 7).alias("is_weekend"),
            "source_folder_loaded",
        )
    )
    overwrite_table(dim, cfg, "dim_date")


def dim_time_df(spark):
    """dim_time   grain: one row per minute of the day (1,440 rows)   key: time_key = HHMM

    Facts carry request_time_key / impression_time_key / postback_time_key (UTC) next to their
    date, so date and time are two small dimensions instead of one row per date x minute.
    """
    m = spark.range(24 * 60).select(F.col("id").cast("int").alias("minute_of_day"))
    hour = (F.col("minute_of_day") / 60).cast("int")
    return m.select(
        (hour * 100 + F.col("minute_of_day") % 60).cast("int").alias("time_key"),
        hour.alias("hour"),
        (F.col("minute_of_day") % 60).cast("int").alias("minute"),
        "minute_of_day",
        F.format_string("%02d:%02d", hour, F.col("minute_of_day") % 60).alias("time_label"),
        F.format_string("%02d:00-%02d:59", hour, hour).alias("hour_label"),
        F.when(hour < 12, "AM").otherwise("PM").alias("am_pm"),
        F.when(hour < 6, "night").when(hour < 12, "morning").when(hour < 18, "afternoon")
        .otherwise("evening").alias("part_of_day"),
    )


def build_dim_time(spark, cfg):
    overwrite_table(dim_time_df(spark), cfg, "dim_time")


def build_dims(spark, cfg):
    build_dim_placement(spark, cfg)
    build_dim_banner(spark, cfg)
    build_dim_date(spark, cfg)
    build_dim_time(spark, cfg)
