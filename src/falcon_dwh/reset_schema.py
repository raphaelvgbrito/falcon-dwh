"""Drop every table in the configured warehouse schema (for a clean full run). Volumes are not touched.

    python -m falcon_dwh.reset_schema [--config path]
"""
import argparse

from falcon_dwh.config import load_config
from falcon_dwh.spark_session import get_spark


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--config")
    args = p.parse_args(argv)
    cfg = load_config(args.config)
    spark = get_spark(cfg)
    wh = cfg["warehouse"]
    schema = f"{wh['catalog']}.{wh['schema']}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    for t in spark.sql(f"SHOW TABLES IN {schema}").collect():
        spark.sql(f"DROP TABLE IF EXISTS {schema}.{t.tableName}")
        print(f"dropped {schema}.{t.tableName}")
    print(f"schema {schema} is empty")


if __name__ == "__main__":
    main()
