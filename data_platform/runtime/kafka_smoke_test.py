import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

TOPICS = [
    "abassurance.assurances.dbo.Policy",
    "assureplus_v2.ASSUREPLUS.POLICY_CONTRACT",
]

def main():
    spark = (
        SparkSession.builder
        .appName("FEMA-Kafka-CDC-Smoke-Test")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        for topic in TOPICS:
            print(f"\n=== {topic} ===", flush=True)

            df = (
                spark.read
                .format("kafka")
                .option("kafka.bootstrap.servers", BROKER)
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .option("endingOffsets", "latest")
                .load()
            )

            df.select(
                "topic",
                "partition",
                "offset",
                "timestamp",
                F.hex("key").alias("key_hex"),
                F.get_json_object(
                    F.col("value").cast("string"),
                    "$.payload.op"
                ).alias("op"),
                F.get_json_object(
                    F.col("value").cast("string"),
                    "$.payload.source"
                ).alias("source_metadata"),
                F.get_json_object(
                    F.col("value").cast("string"),
                    "$.payload.before"
                ).isNotNull().alias("has_before"),
                F.get_json_object(
                    F.col("value").cast("string"),
                    "$.payload.after"
                ).isNotNull().alias("has_after"),
            ).orderBy(F.desc("offset")).limit(3).show(
                truncate=False
            )


    finally:
        spark.stop()


if __name__ == "__main__":
    main()
