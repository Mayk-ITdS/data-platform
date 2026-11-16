"""FEMA Kafka/Debezium -> Apache Iceberg Bronze, streaming ingestion.
Use a distinct checkpoint when first starting this unified consumer. With an
existing populated Bronze table, default `latest` avoids replaying all Kafka
history (and thus duplicating existing topic/partition/offset events).
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TOPICS = [
    "abassurance.assurances.dbo.Building",
    "abassurance.assurances.dbo.Claims",
    "abassurance.assurances.dbo.Holder",
    "abassurance.assurances.dbo.HolderClassification",
    "abassurance.assurances.dbo.Policy",
    "abassurance.assurances.dbo.PolicyRating",
    "abassurance.assurances.dbo.PolicyTerms",
    "abassurance.assurances.dbo.PolicyUnit",
    "abassurance.assurances.dbo.PremiumFinancial",
    "abassurance.assurances.dbo.PrivateClient",
    "abassurance.assurances.dbo.Property",
    "abassurance.assurances.dbo.PropertyUsage",
    "abassurance.assurances.dbo.Risks",
    "assureplus_v2.ASSUREPLUS.CLAIM",
    "assureplus_v2.ASSUREPLUS.HOLDER",
    "assureplus_v2.ASSUREPLUS.POLICY",
    "assureplus_v2.ASSUREPLUS.POLICY_BUILDING_RISK",
    "assureplus_v2.ASSUREPLUS.POLICY_BUILDING_TECHNICAL",
    "assureplus_v2.ASSUREPLUS.POLICY_CONTRACT",
    "assureplus_v2.ASSUREPLUS.POLICY_HOLDER_CLASSIFICATION",
    "assureplus_v2.ASSUREPLUS.POLICY_PREMIUM_FINANCIAL",
    "assureplus_v2.ASSUREPLUS.POLICY_PROPERTY_LOCATION",
    "assureplus_v2.ASSUREPLUS.POLICY_PROPERTY_USAGE",
    "assureplus_v2.ASSUREPLUS.POLICY_RATING",
    "assureplus_v2.ASSUREPLUS.PRIVATE_CLIENT",
]

BRONZE_TABLE = "lakehouse.bronze.cdc_events"
CHECKPOINT = os.getenv("BRONZE_CHECKPOINT", "/lakehouse/checkpoints/bronze_unified_v1")
STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")

def main():
    bootstrap = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    if STARTING_OFFSETS not in {"earliest", "latest"}:
        raise ValueError("KAFKA_STARTING_OFFSETS must be earliest or latest")

    spark = (
        SparkSession.builder
        .appName("FEMA-Bronze-Kafka-Unified")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", "/lakehouse/warehouse")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
        if not spark.catalog.tableExists(BRONZE_TABLE):
            raise RuntimeError(
                f"{BRONZE_TABLE} is missing; create it with the established Bronze schema "
                "before starting this consumer."
            )
        raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap)
            .option("subscribe", ",".join(TOPICS))
            .option("startingOffsets", STARTING_OFFSETS)
            .option("failOnDataLoss", "true")
            .option("maxOffsetsPerTrigger", int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "10000")))
            .load()
        )
        bronze = raw.select(
            F.when(F.col("topic").startswith("abassurance."), F.lit("SQLSERVER"))
            .otherwise(F.lit("ORACLE")).alias("source_system"),
            "topic", "partition", "offset",
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("key").alias("key_raw"),
            F.col("value").alias("value_raw"),
            F.current_timestamp().alias("ingested_at"),
        )
        writer = (
            bronze.writeStream.format("iceberg")
            .outputMode("append")
            .option("checkpointLocation", CHECKPOINT)
        )
        mode = os.getenv("BRONZE_TRIGGER", "availableNow")
        if mode == "availableNow":
            writer = writer.trigger(availableNow=True)
        elif mode == "processingTime":
            writer = writer.trigger(processingTime=os.getenv("BRONZE_PROCESSING_INTERVAL", "30 seconds"))
        else:
            raise ValueError("BRONZE_TRIGGER must be availableNow or processingTime")
        query = writer.toTable(BRONZE_TABLE)
        query.awaitTermination()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
