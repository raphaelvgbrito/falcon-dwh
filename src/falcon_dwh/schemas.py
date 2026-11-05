"""Explicit schemas for every input file, plus where each file lives in a day folder.

Staging rule: keep the data as delivered. So
- CSV booleans ('t'/'f') stay strings; Spark cannot parse them as booleans and would null them.
- requests.data stays the raw JSON text (a string); it is parsed in the warehouse layer.
- CSV / JSON rows that do not fit the schema are kept whole in _corrupt_record, not dropped.
"""
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
)

CORRUPT = "_corrupt_record"


def _s(*fields):
    return StructType([StructField(name, dtype, True) for name, dtype in fields])


S, D, B, TS = StringType(), DoubleType(), BooleanType(), TimestampType()
# Parquet files store timestamps without a time zone (UTC by convention), so they read as NTZ.
TS_NTZ = TimestampNTZType()

# --- event tables -----------------------------------------------------------
REQUESTS = _s(
    ("request_id", S), ("placement_id", S), ("created_at", TS), ("customer_id", S),
    ("session_id", S), ("user_agent", S), ("city", S), ("state", S), ("country", S),
    ("has_preview", B),
    ("data", S),  # nested JSON object kept as its raw text
    (CORRUPT, S),
)
IMPRESSIONS = _s(
    ("impression_id", S), ("fid", S), ("placement_id", S), ("banner_id", S),
    ("created_at", TS_NTZ), ("status", S),
)
CLICKS = _s(("click_id", S), ("fid", S), ("created_at", TS_NTZ), ("status", S))
POSTBACKS = _s(
    ("postback_id", S), ("fid", S), ("created_at", TS_NTZ), ("revenue", D), ("postback_source", S),
)
ORDERS = _s(
    ("order_id", S), ("site_id", S), ("placement_id", S), ("customer_id", S),
    ("created_at", TS), ("amount", D), ("currency", S), ("payment_type", S), (CORRUPT, S),
)

# --- dimension snapshots (CSV) ----------------------------------------------
PLACEMENT = _s(
    ("id", S), ("name", S), ("type", S), ("template", S), ("is_test_mode", S), ("site_id", S),
    ("surface_type", S), ("user_initiated", S), ("created_at", TS), ("updated_at", TS), (CORRUPT, S),
)
SITE = _s(
    ("id", S), ("name", S), ("domain", S), ("status", S), ("ads_enabled", S), ("category_id", S),
    ("publisher_id", S), ("order_volume", D), ("average_order", D), ("created_at", TS),
    ("updated_at", TS), (CORRUPT, S),
)
PUBLISHER = _s(
    ("id", S), ("name", S), ("status", S), ("ads_enabled", S), ("seller", S),
    ("rev_share_percentage", D), ("platform_id", S), ("created_at", TS), ("updated_at", TS),
    (CORRUPT, S),
)
PLATFORM = _s(("id", S), ("name", S), ("is_active", S), (CORRUPT, S))
CATEGORY = _s(("id", S), ("name", S), ("parent_category_id", S), (CORRUPT, S))
ADVERTISER = _s(
    ("id", S), ("name", S), ("payout", D), ("created_at", TS), ("updated_at", TS), (CORRUPT, S),
)
CAMPAIGN = _s(
    ("id", S), ("name", S), ("category_id", S), ("weight", D), ("priority", S), ("cpa", D),
    ("cpa_source", S), ("advertiser_id", S), ("created_at", TS), ("updated_at", TS), (CORRUPT, S),
)
BANNER = _s(
    ("id", S), ("name", S), ("title", S), ("weight", D), ("campaign_id", S), ("is_active", S),
    ("banner_type", S), ("created_at", TS), ("updated_at", TS), (CORRUPT, S),
)

# --- enrichment snapshots (JSON lines) --------------------------------------
_ENRICH_ATTRS = [
    ("created_at", TS), ("updated_at", TS), ("resolved_domain", S), ("main_category", S),
    ("subcategory", S), ("desktop_traffic_share", D), ("mobile_traffic_share", D),
    ("male_share", D), ("female_share", D), ("age_18_to_24", D), ("age_25_to_34", D),
    ("age_35_to_44", D), ("age_45_to_54", D), ("age_55_to_64", D), ("age_65_plus", D),
    ("top_country", S),
]
SITE_ENRICHMENT = _s(("id", S), ("site_id", S), *_ENRICH_ATTRS, (CORRUPT, S))
CAMPAIGN_ENRICHMENT = _s(("id", S), ("campaign_id", S), *_ENRICH_ATTRS, (CORRUPT, S))


# name -> (format, path inside the day folder, schema, kind)
# kind: "event" = only that day's rows, "snapshot" = full copy of the table as of that day
SOURCES = {
    "requests": ("json", "requests", REQUESTS, "event"),
    "impressions": ("parquet", "impressions", IMPRESSIONS, "event"),
    "clicks": ("parquet", "clicks", CLICKS, "event"),
    "postbacks": ("parquet", "postbacks", POSTBACKS, "event"),
    "orders": ("csv", "orders.csv", ORDERS, "event"),
    "placement": ("csv", "dims/placement.csv", PLACEMENT, "snapshot"),
    "site": ("csv", "dims/site.csv", SITE, "snapshot"),
    "publisher": ("csv", "dims/publisher.csv", PUBLISHER, "snapshot"),
    "platform": ("csv", "dims/platform.csv", PLATFORM, "snapshot"),
    "category": ("csv", "dims/category.csv", CATEGORY, "snapshot"),
    "advertiser": ("csv", "dims/advertiser.csv", ADVERTISER, "snapshot"),
    "campaign": ("csv", "dims/campaign.csv", CAMPAIGN, "snapshot"),
    "banner": ("csv", "dims/banner.csv", BANNER, "snapshot"),
    "site_enrichment": ("json", "enrichment/site_enrichment.jsonl", SITE_ENRICHMENT, "snapshot"),
    "campaign_enrichment": (
        "json", "enrichment/campaign_enrichment.jsonl", CAMPAIGN_ENRICHMENT, "snapshot",
    ),
}
