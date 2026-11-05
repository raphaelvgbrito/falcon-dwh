"""History features, built only from what had ARRIVED before the decision day.

For a request on day d, a feature over N days counts events whose folder (load_date) is in
[d-N, d-1]. The folder of day d itself is only complete at the end of d, so it is excluded.
Counting by arrival, not by event time, is what makes late postbacks safe: a conversion for a
day-2 impression that arrives on day 10 is first usable on day 11.

    impressions = slots that arrived (first folder of the fid)
    clicks      = clicked slots, dated by the arrival of their first click
    conversions = de-duplicated postbacks, dated by their first delivery
    ctr = clicks / impressions,  cvr = conversions / impressions   (null when no impressions)
"""
from pyspark.sql import Window
from pyspark.sql import functions as F

from falcon_dwh.config import table_name
from falcon_dwh.facts import postbacks_dedup


def daily_arrivals(spark, cfg, key):
    """(key, arrival_date, impressions, clicks, conversions), key = publisher_id / campaign_id."""
    fi = spark.table(table_name(cfg, "fact_impression")).select("fid", key, "first_load_date")
    owner = fi.select("fid", key)

    imps = fi.groupBy(key, F.col("first_load_date").alias("arrival_date")).agg(F.count("*").alias("impressions"))
    first_click = (
        spark.table(table_name(cfg, "stg_clicks")).groupBy("fid").agg(F.min("load_date").alias("arrival_date"))
        .join(owner, "fid")
        .groupBy(key, "arrival_date").agg(F.count("*").alias("clicks"))
    )
    conv = (
        postbacks_dedup(spark, cfg).join(owner, "fid")
        .groupBy(key, F.col("first_load_date").alias("arrival_date")).agg(F.count("*").alias("conversions"))
    )
    return (
        imps.join(first_click, [key, "arrival_date"], "full")
        .join(conv, [key, "arrival_date"], "full")
        .where(F.col(key).isNotNull())
        .fillna(0, ["impressions", "clicks", "conversions"])
    )


def rolling_rates(daily, key, windows=(7, 28), prefix=None):
    """Feature value for every (key, day) on a full calendar: sums over the previous N days only.

    The calendar is needed because a request can fall on a day with no arrivals of its own.
    rangeBetween(-N, -1) over the day number = the N days before, never the day itself.
    """
    prefix = prefix or key.replace("_id", "")
    bounds = daily.agg(F.min("arrival_date").alias("lo"), F.max("arrival_date").alias("hi"))
    days = bounds.select(F.explode(F.sequence("lo", F.date_add("hi", 1))).alias("feature_date"))
    grid = daily.select(key).distinct().crossJoin(days)
    d = grid.join(
        daily.withColumnRenamed("arrival_date", "feature_date"), [key, "feature_date"], "left"
    ).fillna(0, ["impressions", "clicks", "conversions"])
    d = d.withColumn("_day", F.datediff("feature_date", F.lit("1970-01-01")))

    cols = [key, "feature_date"]
    for n in windows:
        w = Window.partitionBy(key).orderBy("_day").rangeBetween(-n, -1)
        imp, clk, cnv = (F.sum(c).over(w) for c in ("impressions", "clicks", "conversions"))
        d = (
            d.withColumn(f"{prefix}_impressions_{n}d", F.coalesce(imp, F.lit(0)))
            .withColumn(f"{prefix}_ctr_{n}d", F.when(imp > 0, clk / imp))
            .withColumn(f"{prefix}_cvr_{n}d", F.when(imp > 0, cnv / imp))
        )
        cols += [f"{prefix}_impressions_{n}d", f"{prefix}_ctr_{n}d", f"{prefix}_cvr_{n}d"]
    return d.select(*cols)


def history_features(spark, cfg):
    pub = rolling_rates(daily_arrivals(spark, cfg, "publisher_id"), "publisher_id")
    camp = rolling_rates(daily_arrivals(spark, cfg, "campaign_id"), "campaign_id")
    return pub, camp
