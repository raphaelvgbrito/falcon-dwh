"""Table fingerprints, used to prove a re-run does not change the result."""
from pyspark.sql import functions as F


def fingerprint(df, sum_cols=()):
    """Row count, an order-independent checksum of every row, and sums of the given columns.

    The checksum is sum(xxhash64(all columns)), so two DataFrames with the same rows in any
    order give the same value, and any changed, missing or duplicated row changes it.
    """
    aggs = [
        F.count("*").alias("rows"),
        F.coalesce(F.sum(F.xxhash64(*df.columns).cast("decimal(38,0)")), F.lit(0)).alias("checksum"),
    ]
    # decimal sums are exact; a sum of doubles depends on the order Spark adds rows
    aggs += [F.sum(F.col(c).cast("decimal(38,6)")).alias(f"sum_{c}") for c in sum_cols]
    return df.agg(*aggs).first().asDict()
