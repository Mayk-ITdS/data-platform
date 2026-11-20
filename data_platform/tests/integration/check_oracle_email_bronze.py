from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder
    .appName("FEMA-Oracle-Email-Bronze-Diagnostic")
    .config(
        "spark.sql.catalog.lakehouse",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config("spark.sql.catalog.lakehouse.type", "hadoop")
    .config(
        "spark.sql.catalog.lakehouse.warehouse",
        "/lakehouse/warehouse"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

topic = "assureplus_v2.ASSUREPLUS.PRIVATE_CLIENT"

try:
    bronze = (
        spark.table("lakehouse.bronze.cdc_events")
        .where(F.col("topic") == topic)
        .where(F.col("value_raw").isNotNull())
        .select(
            "offset",
            F.col("value_raw").cast("string").alias("raw")
        )
        .select(
            "offset",
            F.get_json_object("raw", "$.payload.op").alias("op"),
            F.get_json_object(
                "raw", "$.payload.after.EMAIL"
            ).alias("email")
        )
    )

    print("\n=== EMAIL DANS BRONZE ===")

    bronze.groupBy("op").agg(
        F.count("*").alias("events"),
        F.count("email").alias("events_with_email")
    ).show(truncate=False)

    print("\n=== MAPPING DE L'INVENTAIRE ===")

    inventory = (
        spark.table("lakehouse.silver.source_schema_inventory")
        .where(F.col("topic") == topic)
        .where(
            F.lower(F.col("source_column")).contains("email")
            | F.lower(F.col("normalized_column")).contains("email")
        )
    )

    inventory.select(
        "source_column", "normalized_column",
        "connect_type", "logical_type"
    ).show(truncate=False)

    print("\n=== EMAIL DANS SILVER ===")

    silver = (
        spark.table("lakehouse.silver.private_client")
        .where(F.col("source_system") == "ORACLE")
    )

    silver.agg(
        F.count("*").alias("rows"),
        F.count("email").alias("emails_present")
    ).show(truncate=False)

finally:
    spark.stop()
