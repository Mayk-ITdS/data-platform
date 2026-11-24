import base64
import os
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark = (
    SparkSession.builder
    .appName("FEMA-Canonical-Policy-Claim")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    .config(
        "spark.sql.catalog.lakehouse",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config("spark.sql.catalog.lakehouse.type", os.getenv("FEMA_CATALOG_TYPE", "hadoop"))
    .config(
        "spark.sql.catalog.lakehouse.warehouse",
        os.getenv("FEMA_WAREHOUSE", "/lakehouse/warehouse")
    )
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

@F.udf(T.StringType())
def oracle_integer(encoded):
    """Decode Debezium Oracle NUMBER(scale=0)."""
    if encoded is None:
        return None

    binary = base64.b64decode(encoded)
    return str(int.from_bytes(binary, byteorder="big", signed=True))

def field(name):
    return F.get_json_object(
        F.col("after_json"), f"$.{name}"
    )

def key_field(name):
    """The key survives both UPDATE and DELETE events."""
    return F.coalesce(
        F.get_json_object(F.col("after_json"), f"$.{name}"),
        F.get_json_object(F.col("before_json"), f"$.{name}")
    )

def source_events(topic):
    return (
        spark.table("lakehouse.bronze.cdc_events")
        .where(F.col("topic") == topic)
        .where(F.col("value_raw").isNotNull())
        .select(
            "source_system",
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            F.col("value_raw").cast("string").alias("json")
        )
        .withColumn(
            "op",
            F.get_json_object("json", "$.payload.op")
        )
        .where(F.col("op").isin("r", "c", "u", "d"))
        .withColumn(
            "after_json",
            F.get_json_object("json", "$.payload.after")
        )
        .withColumn(
            "before_json",
            F.get_json_object("json", "$.payload.before")
        )
    )

def latest_state(df, id_column):
    """
    Reconstruct latest record from Kafka CDC.

    In the current deployment each source table has
    one Kafka partition. Ordering is therefore by offset.
    """
    w = (
        Window
        .partitionBy("source_system", id_column)
        .orderBy(F.col("offset").desc())
    )

    return (
        df.withColumn("_rn", F.row_number().over(w))
        .where(F.col("_rn") == 1)
        .drop("_rn")
        .where(F.col("op") != "d")
    )

def write_table(df, table_name):
    if df.limit(1).count() == 0:
        raise RuntimeError(f"Empty output: {table_name}")

    (
        df.writeTo(f"lakehouse.silver.{table_name}")
        .using("iceberg")
        .createOrReplace()
    )

    print(f"Written: lakehouse.silver.{table_name}")

sql_policy = (
    source_events("abassurance.assurances.dbo.Policy")
    .withColumn("policy_record_id", key_field("PolicyId"))
    .withColumn("holder_id", field("HolderId"))
    .withColumn("holder_context", F.lit(None).cast("string"))
)

oracle_policy = (
    source_events("assureplus_v2.ASSUREPLUS.POLICY")
    .withColumn(
        "policy_record_id",
        oracle_integer(key_field("POLICY_RECORD_ID"))
    )
    .withColumn("holder_id", F.lit(None).cast("string"))
    .withColumn("holder_context", field("HOLDER_CONTEXT"))
)

policy = latest_state(
    sql_policy.unionByName(oracle_policy),
    "policy_record_id"
).select(
    F.concat_ws(
        ":",
        "source_system",
        "policy_record_id"
    ).alias("canonical_policy_key"),
    "source_system",
    "policy_record_id",
    "holder_id",
    "holder_context",
    F.col("op").alias("last_cdc_operation"),
    F.col("topic").alias("source_topic"),
    F.col("partition").alias("source_partition"),
    F.col("offset").alias("source_offset"),
    F.col("kafka_timestamp").alias("source_kafka_timestamp"),
    F.col("after_json").alias("source_record_json")
)

sql_claim = (
    source_events("abassurance.assurances.dbo.Claims")
    .withColumn("claim_id", key_field("ClaimId"))
    .withColumn("policy_record_id", field("PolicyId"))
    .withColumn(
        "date_of_loss",
        F.date_add(
            F.lit("1970-01-01").cast("date"),
            field("DateOfLoss").cast("int")
        )
    )
)

oracle_claim = (
    source_events("assureplus_v2.ASSUREPLUS.CLAIM")
    .withColumn(
        "claim_id",
        oracle_integer(key_field("CLAIM_ID"))
    )
    .withColumn(
        "policy_record_id",
        oracle_integer(field("POLICY_RECORD_ID"))
    )
    .withColumn(
        "date_of_loss",
        F.to_date(
            F.from_unixtime(
                field("DATE_OF_LOSS").cast("double") / 1000
            )
        )
    )
)

claim = latest_state(
    sql_claim.unionByName(oracle_claim),
    "claim_id"
).select(
    F.concat_ws(
        ":", "source_system", "claim_id"
    ).alias("canonical_claim_key"),
    F.concat_ws(
        ":", "source_system", "policy_record_id"
    ).alias("canonical_policy_key"),
    "source_system",
    "claim_id",
    "policy_record_id",
    "date_of_loss",
    F.col("op").alias("last_cdc_operation"),
    F.col("topic").alias("source_topic"),
    F.col("partition").alias("source_partition"),
    F.col("offset").alias("source_offset"),
    F.col("kafka_timestamp").alias("source_kafka_timestamp"),
    F.col("after_json").alias("source_record_json")
)

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")

write_table(policy, "policy")
write_table(claim, "claim")

spark.stop()