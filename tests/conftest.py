"""Shared fixtures. Tests run on the Spark described by the config, but write to a separate
schema (FALCON_TEST_SCHEMA, default falcon_test) so they never touch the real warehouse."""
import os

import pytest

from falcon_dwh.config import load_config
from falcon_dwh.spark_session import get_spark


@pytest.fixture(scope="session")
def cfg():
    cfg = load_config()
    cfg["warehouse"]["schema"] = os.environ.get("FALCON_TEST_SCHEMA", "falcon_test")
    return cfg


@pytest.fixture(scope="session")
def spark(cfg):
    spark = get_spark(cfg)
    wh = cfg["warehouse"]
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {wh['catalog']}.{wh['schema']}")
    return spark
