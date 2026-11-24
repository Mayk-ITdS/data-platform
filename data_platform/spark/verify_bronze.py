import os
from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder
    .appName("FEMA-Verify-Bronze")
    .config(
        "spark.sql.catalog.lakehouse",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config("spark.sql.catalog.lakehouse.type", "hadoop")
    .config(
        "spark.sql.catalog.lakehouse.warehouse",
        os.getenv("FEMA_WAREHOUSE", "/lakehouse/warehouse")
    )
    .getOrCreate()
)

try:
    event = (
        spark.table("lakehouse.bronze.cdc_events")
        .filter(
            (F.col("topic") == "assureplus_v2.ASSUREPLUS.PRIVATE_CLIENT")
            & (F.col("partition") == 0)
            & (F.col("offset") == 10000)
        )
    )

    count = event.count()
    print(f"BRONZE_EVENT_COUNT={count}")

    event.select(
        "source_system",
        "topic",
        "partition",
        "offset",
        "kafka_timestamp",
        "ingested_at"
    ).show(truncate=False)

    if count != 1:
        raise AssertionError(
            f"Expected exactly one event, found {count}"
        )

    print("PASS: Kafka event persisted exactly once in Iceberg Bronze")

finally:
    spark.stop()
