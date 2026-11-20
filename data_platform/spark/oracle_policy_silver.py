import base64
from decimal import Decimal

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, DecimalType


BRONZE_TABLE = "lakehouse.bronze.cdc_events"
SILVER_TABLE = "lakehouse.silver.oracle_policy"

POLICY_TOPIC = "assureplus_v2.ASSUREPLUS.POLICY"
CONTRACT_TOPIC = "assureplus_v2.ASSUREPLUS.POLICY_CONTRACT"


spark = (
    SparkSession.builder
    .appName("FEMA-Oracle-Policy-Silver")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    .config(
        "spark.sql.catalog.lakehouse",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config("spark.sql.catalog.lakehouse.type", "hadoop")
    .config(
        "spark.sql.catalog.lakehouse.warehouse",
        "/lakehouse/warehouse"
    )
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

@F.udf(LongType())
def decode_id(value):
    if value is None:
        return None

    raw = base64.b64decode(value)
    return int.from_bytes(raw, byteorder="big", signed=True)

@F.udf(DecimalType(20, 4))
def decode_money(value):
    if value is None:
        return None

    raw = base64.b64decode(value)
    unscaled = int.from_bytes(raw, byteorder="big", signed=True)

    return Decimal(unscaled).scaleb(-4)

bronze = (
    spark.table(BRONZE_TABLE)
    .filter(F.col("topic").isin(POLICY_TOPIC, CONTRACT_TOPIC))
    .select(
        "topic",
        "partition",
        "offset",
        F.col("key_raw").cast("string").alias("key_json"),
        F.col("value_raw").cast("string").alias("value_json"),
    )
)

def json_field(path):
    return F.get_json_object(F.col("value_json"), path)


events = (
    bronze
    .withColumn(
        "policy_record_id",
        decode_id(
            F.get_json_object(
                F.col("key_json"),
                "$.payload.POLICY_RECORD_ID"
            )
        )
    )
    .withColumn("op", json_field("$.payload.op"))
)

invalid_keys = events.filter(
    F.col("policy_record_id").isNull()
).count()

if invalid_keys:
    raise RuntimeError(
        f"Invalid POLICY_RECORD_ID in {invalid_keys} CDC events"
    )

def latest_state(df):
    df = df.filter(F.col("op").isNotNull())

    window = (
        Window
        .partitionBy("policy_record_id")
        .orderBy(F.col("offset").desc())
    )

    return (
        df.withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .filter(F.col("op") != "d")
    )

policy = latest_state(
    events.filter(F.col("topic") == POLICY_TOPIC)
)

policy = policy.select(
    "policy_record_id",
    json_field("$.payload.after.HOLDER_CONTEXT")
        .alias("holder_context"),
    json_field("$.payload.after.SOURCE_SYSTEM")
        .alias("source_system"),
    json_field("$.payload.after.ORIGINAL_NB_DATE")
        .cast("long")
        .alias("original_nb_date_ms"),
)

contract = latest_state(
    events.filter(F.col("topic") == CONTRACT_TOPIC)
)

contract = contract.select(
    "policy_record_id",
    decode_money(
        json_field("$.payload.after.POLICY_COST")
    ).alias("policy_cost"),

    decode_money(
        json_field("$.payload.after.TOTAL_BUILDING_COVERAGE")
    ).alias("total_building_coverage"),

    decode_money(
        json_field("$.payload.after.TOTAL_CONTENTS_COVERAGE")
    ).alias("total_contents_coverage"),

    json_field("$.payload.after.POLICY_EFFECTIVE_DATE")
        .cast("long")
        .alias("policy_effective_ms"),

    json_field("$.payload.after.POLICY_TERMINATION_DATE")
        .cast("long")
        .alias("policy_termination_ms"),

    json_field("$.payload.after.POLICY_COUNT")
        .cast("long")
        .alias("policy_count"),
)

silver = (
    policy.alias("p")
    .join(
        contract.alias("c"),
        on="policy_record_id",
        how="left"
    )
    .withColumn(
        "policy_effective_date",
        F.timestamp_millis("policy_effective_ms")
    )
    .withColumn(
        "policy_termination_date",
        F.timestamp_millis("policy_termination_ms")
    )
    .withColumn(
        "original_nb_date",
        F.timestamp_millis("original_nb_date_ms")
    )
    .drop(
        "policy_effective_ms",
        "policy_termination_ms",
        "original_nb_date_ms"
    )
    .withColumn("silver_updated_at", F.current_timestamp())
)

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")

(
    silver.writeTo(SILVER_TABLE)
    .using("iceberg")
    .createOrReplace()
)

result = spark.table(SILVER_TABLE)

result.select(
    F.count("*").alias("policies"),
    F.countDistinct("policy_record_id").alias("unique_policies"),
    F.count("policy_cost").alias("policies_with_cost"),
).show(truncate=False)

result.filter(
    F.col("policy_record_id") == 3039
).select(
    "policy_record_id",
    "policy_cost",
    "policy_effective_date",
    "policy_termination_date",
).show(truncate=False)

spark.stop()
