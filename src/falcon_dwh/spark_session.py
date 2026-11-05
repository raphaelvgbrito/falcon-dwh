"""Create the SparkSession described by the config."""


def get_spark(cfg):
    sc = cfg["spark"]
    if sc["mode"] == "databricks_connect":
        from databricks.connect import DatabricksSession

        builder = DatabricksSession.builder
        if sc.get("databricks_profile"):
            builder = builder.profile(sc["databricks_profile"])
        if sc.get("serverless", True):
            builder = builder.serverless(True)
        else:
            builder = builder.clusterId(sc["cluster_id"])
        spark = builder.getOrCreate()
    elif sc["mode"] == "cluster":
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
    else:
        raise ValueError(f"unknown spark.mode: {sc['mode']}")

    for key, value in (sc.get("session_conf") or {}).items():
        spark.conf.set(key, value)
    return spark
