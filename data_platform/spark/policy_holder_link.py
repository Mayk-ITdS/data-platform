"""Link source policies to canonical holders without changing policy grain."""

import os
from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder
    .appName("FEMA-Policy-Holder-Link")
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
        os.getenv("FEMA_WAREHOUSE", "/lakehouse/warehouse")
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


def assert_unique(df, keys, label):
    duplicates = (
        df.groupBy(*keys)
        .count()
        .where(F.col("count") > 1)
    )
    if duplicates.limit(1).count():
        raise AssertionError(f"Duplicate keys in {label}")


try:
    policy = spark.table("lakehouse.silver.policy")

    links = spark.table(
        "lakehouse.silver_identity_work.holder_source_link_v1"
    )

    assert_unique(
        policy,
        ["canonical_policy_key"],
        "silver.policy"
    )

    assert_unique(
        links,
        ["source_system", "source_holder_context"],
        "holder_source_link"
    )

    assert_unique(
        links.where(F.col("source_system") == "SQLSERVER"),
        ["source_system", "source_holder_id"],
        "SQL Server source holders"
    )

    # SQL Server: Policy.HolderId -> Holder.HolderId
    sql_policies = (
        policy
        .where(F.col("source_system") == "SQLSERVER")
        .alias("p")
    )

    sql_holders = (
        links
        .where(F.col("source_system") == "SQLSERVER")
        .alias("h")
    )

    sql_result = (
        sql_policies.join(
            sql_holders,
            F.col("p.holder_id").cast("string")
            == F.col("h.source_holder_id"),
            "left"
        )
        .select(
            F.col("p.canonical_policy_key"),
            F.col("p.source_system"),
            F.col("p.holder_id").cast("string")
             .alias("source_holder_reference"),
            F.col("h.canonical_holder_id")
        )
    )

    # Oracle: POLICY.HOLDER_CONTEXT -> HOLDER.HOLDER_CONTEXT
    oracle_policies = (
        policy
        .where(F.col("source_system") == "ORACLE")
        .alias("p")
    )

    oracle_holders = (
        links
        .where(F.col("source_system") == "ORACLE")
        .alias("h")
    )

    oracle_result = (
        oracle_policies.join(
            oracle_holders,
            F.col("p.holder_context").cast("string")
            == F.col("h.source_holder_context"),
            "left"
        )
        .select(
            F.col("p.canonical_policy_key"),
            F.col("p.source_system"),
            F.col("p.holder_context").cast("string")
             .alias("source_holder_reference"),
            F.col("h.canonical_holder_id")
        )
    )

    result = sql_result.unionByName(oracle_result)

    # Check coverage and cardinality.
    original_count = policy.count()
    result_count = result.count()

    if original_count == 0:
        raise AssertionError("No policies found")

    if original_count != result_count:
        raise AssertionError(
            f"Policy grain changed: {original_count} -> {result_count}"
        )

    assert_unique(
        result,
        ["canonical_policy_key"],
        "policy_holder_link"
    )

    missing = result.where(
        F.col("canonical_holder_id").isNull()
    ).count()

    print("Source policies:", original_count)
    print("Linked rows:", result_count)
    print("Missing holder references:", missing)

    result.groupBy("source_system").agg(
        F.count("*").alias("policies"),
        F.count("canonical_holder_id").alias("linked")
    ).show(truncate=False)

    if missing:
        raise AssertionError(
            f"{missing} policies have no canonical holder"
        )

    # Publish only after validation.
    result.writeTo(
        "lakehouse.silver_identity_work.policy_holder_link_v1"
    ).using("iceberg").createOrReplace()

    print("PASS policy-holder reconciliation")

finally:
    spark.stop()
