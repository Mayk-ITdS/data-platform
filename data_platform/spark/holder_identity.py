"""FEMA Silver — canonical holder identity reconciliation.
Input:
    lakehouse.silver.holder
    lakehouse.silver.private_client
Output (staging):
    lakehouse.silver_identity_work.holder_canonical_v1
    lakehouse.silver_identity_work.holder_source_link_v1
Only holder identities are reconciled. Policies are not modified.
"""

from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder
    .appName("FEMA-Holder-Identity-Reconciliation")
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

spark.sparkContext.setLogLevel("WARN")

def require_columns(df, columns, table):
    missing = set(columns) - set(df.columns)
    if missing:
        raise ValueError(
            f"{table}: missing columns {sorted(missing)}"
        )


def assert_unique(df, columns, label):
    duplicates = (
        df.groupBy(*columns)
        .count()
        .where(F.col("count") > 1)
    )

    if duplicates.limit(1).count():
        raise ValueError(
            f"{label}: duplicate identity keys detected"
        )


try:
    holders = spark.table("lakehouse.silver.holder")
    clients = spark.table("lakehouse.silver.private_client")

    require_columns(
        holders,
        ["source_system", "holder_context"],
        "silver.holder",
    )

    require_columns(
        clients,
        ["source_system", "holder_context", "email"],
        "silver.private_client",
    )

    source_holders = (
        holders.select(
            F.col("source_system"),
            F.col("holder_context").cast("string").alias(
                "source_holder_context"
            ),
            F.col("source_primary_key").cast("string").alias(
                "source_holder_id"
            ),
        )
    )

    if source_holders.where(
        F.col("source_holder_context").isNull()
    ).limit(1).count():
        raise ValueError("HolderContext cannot be null")

    assert_unique(
        source_holders,
        ["source_system", "source_holder_context"],
        "Source holders",
    )

    email_sql = (
        F.col("email")
        if "email" in clients.columns
        else F.lit(None).cast("string")
    )

    email_oracle = (
        F.col("ap_email")
        if "ap_email" in clients.columns
        else F.lit(None).cast("string")
    )

    client_emails = (
        clients.select(
            "source_system",
            F.col("holder_context")
            .cast("string")
            .alias("source_holder_context"),
            F.lower(
                F.trim(
                    F.when(
                        F.col("source_system") == "ORACLE",
                        email_oracle
                    ).otherwise(email_sql)
                )
            ).alias("email_normalized")
        )
        .withColumn(
            "email_normalized",
            F.when(
                F.col("email_normalized") == "",
                F.lit(None).cast("string")
            ).otherwise(F.col("email_normalized"))
        )
    )
    client_emails.groupBy("source_system").agg(
        F.count("*").alias("clients"),
        F.count("email_normalized").alias("emails_present")
    ).show(truncate=False)
    assert_unique(
        client_emails,
        ["source_system", "source_holder_context"],
        "PrivateClient",
    )

    linked = source_holders.join(
        client_emails,
        ["source_system", "source_holder_context"],
        "left",
    )

    orphans = client_emails.join(
        source_holders,
        ["source_system", "source_holder_context"],
        "left_anti",
    )

    if orphans.limit(1).count():
        raise ValueError(
            "PrivateClient references an unknown source holder"
        )

    linked = linked.withColumn(
        "canonical_holder_id",
        F.when(
            F.col("email_normalized").isNotNull(),
            F.concat(
                F.lit("EMAIL:"),
                F.sha2(F.col("email_normalized"), 256),
            ),
        ).otherwise(
            F.concat(
                F.lit("SOURCE:"),
                F.col("source_system"),
                F.lit(":"),
                F.col("source_holder_context"),
            )
        ),
    )

    assert_unique(
        linked,
        ["source_system", "source_holder_context"],
        "Holder source links",
    )

    canonical = (
        linked.groupBy("canonical_holder_id")
        .agg(
            F.first(
                "email_normalized",
                ignorenulls=True,
            ).alias("email_normalized"),
            F.count("*").alias("source_identity_count"),
            F.countDistinct("source_system").alias(
                "source_system_count"
            ),
        )
    )

    assert_unique(
        canonical.where(
            F.col("email_normalized").isNotNull()
        ),
        ["email_normalized"],
        "Canonical emails",
    )

    source_count = source_holders.count()
    link_count = linked.count()
    canonical_count = canonical.count()

    if source_count == 0:
        raise RuntimeError(
            "No source holders found; refusing to publish empty canonical tables"
        )

    if canonical_count == 0:
        raise RuntimeError(
            "Canonical holder table is empty"
        )

    if source_count != link_count:
        raise AssertionError(
            f"Holder loss: {source_count} != {link_count}"
        )

    if canonical_count > source_count:
        raise AssertionError(
            "Canonical identity count exceeds source count"
        )

    print(f"Source holders: {source_count}")
    print(f"Source links: {link_count}")
    print(f"Canonical holders: {canonical_count}")
    print(f"Reconciled identities: {source_count - canonical_count}")

    spark.sql(
        "CREATE NAMESPACE IF NOT EXISTS "
        "lakehouse.silver_identity_work"
    )

    canonical.writeTo(
        "lakehouse.silver_identity_work.holder_canonical_v1"
    ).using("iceberg").createOrReplace()

    linked.writeTo(
        "lakehouse.silver_identity_work.holder_source_link_v1"
    ).using("iceberg").createOrReplace()

    print("PASS holder identity reconciliation")

finally:
    spark.stop()
