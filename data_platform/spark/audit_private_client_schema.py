import os
from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder
    .appName("FEMA-PrivateClient-Schema-Audit")
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
    topic = "assureplus_v2.ASSUREPLUS.PRIVATE_CLIENT"

    inventory = (
        spark.table("lakehouse.silver.source_schema_inventory")
        .filter(F.col("topic") == topic)
    )

    print("=== SCHEMA INVENTORY ===")

    inventory.select(
        "source_column",
        "normalized_column",
        "connect_type",
        "logical_type"
    ).show(100, truncate=False)

    print("=== CURRENT SILVER ===")

    silver = (
        spark.table("lakehouse.silver.private_client")
        .filter(F.col("source_system") == "ORACLE")
    )

    print("Oracle rows:", silver.count())
    print("Silver columns:", silver.columns)

    if "email" in silver.columns:
        print(
            "Non-null Oracle emails:",
            silver.filter(F.col("email").isNotNull()).count()
        )

    print("=== CDC TEST EVENT ===")

    event = (
        spark.table("lakehouse.bronze.cdc_events")
        .filter(
            (F.col("topic") == topic)
            & (F.col("partition") == 0)
            & (F.col("offset") == 10000)
        )
    )

    event.select(
        F.get_json_object(
            F.col("value_raw").cast("string"),
            "$.payload.op"
        ).alias("operation"),
        F.get_json_object(
            F.col("value_raw").cast("string"),
            "$.payload.after.AP_EMAIL"
        ).isNotNull().alias("ap_email_present"),
        F.get_json_object(
            F.col("value_raw").cast("string"),
            "$.payload.after.AB_EMAIL"
        ).isNotNull().alias("ab_email_present")
    ).show(truncate=False)

finally:
    spark.stop()
