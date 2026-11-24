"""Build small observable Gold marts from all canonical Silver entities and CDC Bronze.

Runs as a finite Spark batch after Silver reconstruction. It does not implement
incremental Silver MERGE or automatically refresh Power BI by itself.
"""
from datetime import datetime, timezone
from pyspark.sql import SparkSession, functions as F, types as T

SILVER_ENTITIES = (
    "building", "claim", "holder", "holder_classification",
    "policy", "policy_rating", "policy_terms", "policy_unit",
    "premium_financial", "private_client", "property",
    "property_usage", "risks",
)

spark = (
    SparkSession.builder.appName("FEMA-Gold-Demo-Marts")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "hadoop")
    .config("spark.sql.catalog.lakehouse.warehouse", "/lakehouse/warehouse")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
try:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
    now = datetime.now(timezone.utc)
    counts = []
    for name in SILVER_ENTITIES:
        source = f"lakehouse.silver.{name}"
        if not spark.catalog.tableExists(source):
            raise RuntimeError(f"Required Silver table missing: {source}")
        frame = spark.table(source)
        if "source_system" not in frame.columns:
            raise RuntimeError(f"source_system column missing in {source}")
        for row in frame.groupBy("source_system").count().collect():
            counts.append((name, row["source_system"], row["count"], now))
        print("READ:", source, flush=True)

    schema = T.StructType([
        T.StructField("entity", T.StringType(), False),
        T.StructField("source_system", T.StringType(), True),
        T.StructField("record_count", T.LongType(), False),
        T.StructField("refreshed_at", T.TimestampType(), False),
    ])
    summary = spark.createDataFrame(counts, schema)
    if not counts:
        raise RuntimeError("Silver entity summary is empty")

    events = spark.table("lakehouse.bronze.cdc_events")
    topic_activity = events.groupBy("source_system", "topic").agg(
        F.count("*").alias("event_count"),
        F.max("kafka_timestamp").alias("last_event_at"),
        F.max("ingested_at").alias("last_ingested_at"),
    ).withColumn("refreshed_at", F.lit(now))

    # Create separate Iceberg tables; do not overwrite Bronze or Silver.
    summary.writeTo("lakehouse.gold.entity_counts").using("iceberg").createOrReplace()
    topic_activity.writeTo("lakehouse.gold.cdc_topic_activity").using("iceberg").createOrReplace()

    print("GOLD COMPLETE: lakehouse.gold.entity_counts", flush=True)
    print("GOLD COMPLETE: lakehouse.gold.cdc_topic_activity", flush=True)
    print(f"Silver entity/source combinations: {len(counts)}", flush=True)
    print(f"Distinct observed CDC topics: {topic_activity.count()}", flush=True)
finally:
    spark.stop()
