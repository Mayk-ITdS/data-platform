"""Build small observable Gold marts from all canonical Silver entities and CDC Bronze.

Runs as a finite Spark batch after Silver reconstruction. It does not implement
incremental Silver MERGE or automatically refresh Power BI by itself.
"""
from datetime import datetime, timezone
import os
from pyspark.sql import SparkSession, functions as F, types as T

SILVER_ENTITIES = (
    "building", "claim", "holder", "holder_classification",
    "policy", "policy_rating", "policy_terms", "policy_unit",
    "premium_financial", "private_client", "property",
    "property_usage", "risks",
)


def silver(spark, name):
    return spark.table(f"lakehouse.silver.{name}")


def validate_unique(df, key, name):
    invalid = (
        df.groupBy(key)
        .count()
        .filter(F.col(key).isNull() | (F.col("count") > 1))
        .limit(1)
        .count()
    )

    if invalid:
        raise ValueError(f"Invalid key cardinality: {name}.{key}")


def build_business_marts(spark):
    key = "canonical_policy_key"

    policy = silver(spark, "policy").select(
        key, "source_system", "holder_id", "holder_context"
    )

    terms = silver(spark, "policy_terms").select(
        key,
        "policy_cost",
        "total_building_coverage",
        "total_contents_coverage",
        "policy_effective_date",
        "policy_termination_date",
    )

    premium = silver(spark, "premium_financial").select(
        key,
        "total_policy_insurance_premium",
        "federal_policy_fee",
        "hfiaa_surcharge",
        "full_risk_premium",
    )

    property_df = silver(spark, "property").select(
        key, "property_state", "fema_region"
    )

    risks = silver(spark, "risks").select(
        key, "rated_flood_zone", "building_replacement_cost"
    )

    claims = silver(spark, "claim").select(
        "canonical_claim_key", key, "source_system", "date_of_loss"
    )

    holder = silver(spark, "holder").select(
        "canonical_holder_key",
        "source_system",
        "holder_id",
        "holder_context",
    )

    clients = silver(spark, "private_client").select(
        "canonical_holder_key",
        "source_system",
        "loyalty_score",
    )

    for name, df in [
        ("policy", policy),
        ("policy_terms", terms),
        ("premium_financial", premium),
        ("property", property_df),
        ("risks", risks),
    ]:
        validate_unique(df, key, name)

    validate_unique(claims, "canonical_claim_key", "claim")
    validate_unique(holder, "canonical_holder_key", "holder")
    validate_unique(clients, "canonical_holder_key", "private_client")

    claim_counts = claims.groupBy(key).agg(
        F.count("*").alias("claim_count")
    )

    portfolio = (
        policy
        .join(terms, key, "left")
        .join(premium, key, "left")
        .join(claim_counts, key, "left")
        .fillna({"claim_count": 0})
        .groupBy("source_system")
        .agg(
            F.count("*").alias("policy_count"),
            F.sum("policy_cost").alias("total_policy_cost"),
            F.sum("total_policy_insurance_premium").alias(
                "total_premium"
            ),
            F.sum("total_building_coverage").alias(
                "building_coverage"
            ),
            F.sum("total_contents_coverage").alias(
                "contents_coverage"
            ),
            F.sum("claim_count").alias("claim_count"),
            F.sum(
                F.when(F.col("claim_count") > 0, 1).otherwise(0)
            ).alias("policies_with_claims"),
        )
        .withColumn(
            "claim_frequency_pct",
            F.round(
                100.0 * F.col("policies_with_claims")
                / F.col("policy_count"), 2
            ),
        )
    )

    premium_analysis = (
        policy.select(key, "source_system")
        .join(premium, key, "left")
        .join(terms, key, "left")
        .withColumn(
            "policy_year",
            F.year("policy_effective_date")
        )
        .groupBy("source_system", "policy_year")
        .agg(
            F.count("*").alias("policy_count"),
            F.sum("total_policy_insurance_premium").alias(
                "total_premium"
            ),
            F.avg("total_policy_insurance_premium").alias(
                "average_premium"
            ),
            F.sum("federal_policy_fee").alias("total_federal_fee"),
            F.sum("hfiaa_surcharge").alias("total_hfiaa_surcharge"),
            F.sum("full_risk_premium").alias("total_full_risk_premium"),
        )
    )

    claims_analysis = (
        claims
        .withColumn("loss_year", F.year("date_of_loss"))
        .withColumn("loss_month", F.month("date_of_loss"))
        .groupBy("source_system", "loss_year", "loss_month")
        .agg(
            F.count("*").alias("claim_count"),
            F.countDistinct(key).alias("affected_policies"),
        )
    )

    geographic_risk = (
        policy.select(key, "source_system")
        .join(property_df, key, "left")
        .join(risks, key, "left")
        .join(claim_counts, key, "left")
        .fillna({"claim_count": 0})
        .groupBy(
            "source_system",
            "property_state",
            "fema_region",
            "rated_flood_zone",
        )
        .agg(
            F.count("*").alias("policy_count"),
            F.sum("claim_count").alias("claim_count"),
            F.sum("building_replacement_cost").alias(
                "total_building_replacement_cost"
            ),
        )
    )

    sql_holders = holder.filter(
        F.col("source_system") == "SQLSERVER"
    ).select(
        F.col("holder_id").cast("string").alias("holder_id"),
        "canonical_holder_key",
    )

    validate_unique(sql_holders, "holder_id", "sqlserver_holder")

    oracle_holders = holder.filter(
        F.col("source_system") == "ORACLE"
    ).select("holder_context", "canonical_holder_key")

    validate_unique(
        oracle_holders, "holder_context", "oracle_holder"
    )

    sql_policies = (
        policy.filter(F.col("source_system") == "SQLSERVER")
        .join(sql_holders, "holder_id", "left")
        .select(key, "canonical_holder_key")
    )

    oracle_policies = (
        policy.filter(F.col("source_system") == "ORACLE")
        .join(oracle_holders, "holder_context", "left")
        .select(key, "canonical_holder_key")
    )

    policies_per_holder = (
        sql_policies.unionByName(oracle_policies)
        .filter(F.col("canonical_holder_key").isNotNull())
        .groupBy("canonical_holder_key")
        .agg(F.count("*").alias("policy_count"))
    )

    customer_segmentation = (
        clients
        .join(policies_per_holder, "canonical_holder_key", "left")
        .fillna({"policy_count": 0})
        .withColumn(
            "loyalty_score",
            F.least(
                F.lit(100),
                F.round(
                    100.0 * F.log1p(F.col("policy_count"))
                    / F.log(F.lit(21.0))
                )
            ).cast("int")
        )
        .withColumn(
            "customer_segment",
            F.when(F.col("policy_count") == 0, "0")
            .when(F.col("policy_count") == 1, "1")
            .when(F.col("policy_count") <= 5, "2-5")
            .when(F.col("policy_count") <= 10, "6-10")
            .otherwise("11+")
        )
        .groupBy("source_system", "customer_segment")
        .agg(
            F.count("*").alias("customer_count"),
            F.sum("policy_count").alias("policy_count"),
            F.avg("loyalty_score").alias("average_loyalty_score"),
            F.count("loyalty_score").alias("scored_customers"),
        )
    )

    marts = {
        "portfolio_summary": portfolio,
        "premium_analysis": premium_analysis,
        "claims_analysis": claims_analysis,
        "geographic_risk": geographic_risk,
        "customer_segmentation": customer_segmentation,
    }

    for name, df in marts.items():
        target = f"lakehouse.gold.{name}"

        df = df.withColumn("refreshed_at", F.current_timestamp())

        df.writeTo(target).using("iceberg").createOrReplace()

        published = spark.table(target)

        print("=" * 80, flush=True)
        print(f"GOLD MART: {name.upper()}", flush=True)
        print("=" * 80, flush=True)

        published.show(10, truncate=False)

        print(
            f"PASS: {target} - {published.count()} rows",
            flush=True,
        )


spark = (
    SparkSession.builder.appName("FEMA-Gold-Demo-Marts")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", os.getenv("FEMA_CATALOG_TYPE", "hadoop"))
    .config("spark.sql.catalog.lakehouse.warehouse", os.getenv("FEMA_WAREHOUSE", "/lakehouse/warehouse"))
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
            source_system, record_count = row
            counts.append((name, source_system, record_count, now))
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
    build_business_marts(spark)
finally:
    spark.stop()
