import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TOPICS = [
    "abassurance.assurances.dbo.Holder",
    "assureplus_v2.ASSUREPLUS.CLAIM",
    "assureplus_v2.ASSUREPLUS.HOLDER",
    "assureplus_v2.ASSUREPLUS.POLICY",
    "assureplus_v2.ASSUREPLUS.POLICY_BUILDING_RISK",
    "assureplus_v2.ASSUREPLUS.POLICY_BUILDING_TECHNICAL",
    "assureplus_v2.ASSUREPLUS.POLICY_HOLDER_CLASSIFICATION",
    "assureplus_v2.ASSUREPLUS.POLICY_PREMIUM_FINANCIAL",
    "assureplus_v2.ASSUREPLUS.POLICY_PROPERTY_LOCATION",
    "assureplus_v2.ASSUREPLUS.POLICY_PROPERTY_USAGE",
    "assureplus_v2.ASSUREPLUS.POLICY_RATING",
    "assureplus_v2.ASSUREPLUS.PRIVATE_CLIENT",
]

CHECKPOINT = "/lakehouse/checkpoints/bronze_remaining"


spark = (
    SparkSession.builder
    .appName("FEMA-Bronze-Remaining-Topics")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config(
        "spark.sql.catalog.lakehouse",
        "org.apache.iceberg.spark.SparkCatalog",
    )
    .config("spark.sql.catalog.lakehouse.type", "hadoop")
    .config(
        "spark.sql.catalog.lakehouse.warehouse",
        "/lakehouse/warehouse",
    )
    .getOrCreate()
)

raw = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    )
    .option("subscribe", ",".join(TOPICS))
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 10000)
    .load()
)

bronze = raw.select(
    F.when(
        F.col("topic").startswith("abassurance."),
        F.lit("SQLSERVER"),
    )
    .otherwise(F.lit("ORACLE"))
    .alias("source_system"),
    F.col("topic"),
    F.col("partition"),
    F.col("offset"),
    F.col("timestamp").alias("kafka_timestamp"),
    F.col("key").alias("key_raw"),
    F.col("value").alias("value_raw"),
    F.current_timestamp().alias("ingested_at"),
)

query = (
    bronze.writeStream
    .format("iceberg")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .toTable("lakehouse.bronze.cdc_events")
)

query.awaitTermination()
spark.stop()
