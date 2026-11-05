"""Write helpers. Every write replaces a well-defined slice, never appends, so runs are idempotent."""
import datetime as dt

from falcon_dwh.config import table_name


def overwrite_table(df, cfg, name):
    """Replace the whole table (used for small tables rebuilt from scratch, e.g. dimensions)."""
    (
        df.write.format(cfg["warehouse"]["format"])
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable(table_name(cfg, name))
    )


def overwrite_partitions(df, cfg, name, part_col, dates):
    """Replace only the given date partitions. Delta also checks every written row is inside them."""
    dates = sorted({d.isoformat() if isinstance(d, dt.date) else str(d) for d in dates if d is not None})
    if not dates:
        return
    in_list = ", ".join(f"DATE'{d}'" for d in dates)
    (
        df.write.format(cfg["warehouse"]["format"])
        .mode("overwrite")
        .option("replaceWhere", f"{part_col} IN ({in_list})")
        .partitionBy(part_col)
        .saveAsTable(table_name(cfg, name))
    )


def table_exists(spark, cfg, name):
    return spark.catalog.tableExists(table_name(cfg, name))
