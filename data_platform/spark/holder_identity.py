import os

from pyspark.sql import SparkSession, functions as F


def check_expected(name, actual, env_name):
    raw = os.getenv(env_name)
    if raw is not None:
        expected = int(raw)
        if actual != expected:
            raise ValueError(
                f"{name}: expected {expected}, got {actual}"
            )


def create_spark():
    spark = (
        SparkSession.builder
        .appName("FEMA-Holder-Identity-Candidate")
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

    spark.sparkContext.setLogLevel("ERROR")
    return spark


def norm(column):
    return F.regexp_replace(
        F.lower(F.trim(column.cast("string"))),
        r"\s+", " "
    )


def build_identity(holders, clients):

    keys = ["source_system", "holder_context"]

    # Preserve every source identity.
    source_holders = holders.select(
        "source_system",
        "holder_context",
        "source_primary_key"
    )

    source_count = source_holders.count()
    client_count = clients.count()

    if source_count == 0:
        raise ValueError("No source holders")

    if source_holders.select(*keys).distinct().count() != source_count:
        raise ValueError("Duplicate source holder keys")

    if clients.select(*keys).distinct().count() != client_count:
        raise ValueError("Duplicate client keys")

    # One row in PrivateClient must reference one existing Holder.
    assert clients.join(
        source_holders.select(*keys), keys, "left_anti"
    ).count() == 0

    # Canonical comparison attributes.
    prepared = (
        clients
        .withColumn("email_normalized", norm(F.col("email")))
        .withColumn(
            "identity_name",
            F.when(
                F.col("source_system") == "SQLSERVER",
                norm(F.col("full_name"))
            ).otherwise(
                norm(F.concat_ws(
                    " ",
                    F.col("first_name"),
                    F.col("last_name")
                ))
            )
        )
        .withColumn(
            "identity_state",
            norm(F.coalesce("state", "state_code"))
        )
        .withColumn("identity_sex", norm(F.col("sex")))
        .withColumn(
            "identity_birth",
            F.col("birth_year").cast("long")
        )
    )

    sql = prepared.where(
        F.col("source_system") == "SQLSERVER"
    ).alias("s")

    oracle = prepared.where(
        F.col("source_system") == "ORACLE"
    ).alias("o")

    # Match only fully validated identities.
    pairs = (
        sql.join(
            oracle,
            F.col("s.email_normalized")
            == F.col("o.email_normalized"),
            "inner"
        )
        .where(F.col("s.email_normalized").isNotNull())
        .where(F.col("s.email_normalized") != "")
        .where(
            F.col("s.identity_name").isNotNull()
            & (F.col("s.identity_name") != "")
            & F.col("s.identity_name").eqNullSafe(
                F.col("o.identity_name")
            )
            & F.col("s.identity_birth").isNotNull()
            & F.col("s.identity_birth").eqNullSafe(
                F.col("o.identity_birth")
            )
            & F.col("s.identity_sex").isNotNull()
            & F.col("s.identity_sex").eqNullSafe(
                F.col("o.identity_sex")
            )
            & F.col("s.identity_state").isNotNull()
            & F.col("s.identity_state").eqNullSafe(
                F.col("o.identity_state")
            )
        )
        .select(
            F.col("s.email_normalized").alias("email_normalized"),
            F.col("s.holder_context").alias("sql_context"),
            F.col("o.holder_context").alias("oracle_context")
        )
    )

    pair_count = pairs.count()

    for column in ("sql_context", "oracle_context", "email_normalized"):
        if pairs.select(column).distinct().count() != pair_count:
            raise ValueError(f"Ambiguous matching: {column}")

    approved_emails = pairs.select(
        "email_normalized"
    ).distinct()

    # Attach a canonical ID to every source holder.
    linked = (
        source_holders.join(
            prepared.select(
                *keys, "email_normalized"
            ),
            keys,
            "left"
        )
        .join(
            approved_emails.withColumn(
                "approved", F.lit(True)
            ),
            "email_normalized",
            "left"
        )
        .withColumn(
            "canonical_holder_id",
            F.when(
                F.col("approved") == True,
                F.concat(
                    F.lit("EMAIL:"),
                    F.sha2(F.col("email_normalized"), 256)
                )
            ).otherwise(
                F.concat(
                    F.lit("SOURCE:"),
                    F.col("source_system"),
                    F.lit(":"),
                    F.col("holder_context")
                )
            )
        )
        .drop("approved")
    )

    canonical = linked.groupBy(
        "canonical_holder_id"
    ).agg(
        F.first(
            "email_normalized",
            ignorenulls=True
        ).alias("email_normalized"),
        F.count("*").alias("source_identity_count"),
        F.countDistinct("source_system").alias(
            "source_system_count"
        )
    )

    links_count = linked.count()
    canonical_count = canonical.count()

    cross_system = canonical.where(
        F.col("source_system_count") == 2
    ).count()

    duplicate_links = (
        linked.groupBy(*keys)
        .count()
        .where(F.col("count") > 1)
        .count()
    )

    print("\n=== IDENTITY RECONCILIATION ===")
    print("Source links:", links_count)
    print("Canonical identities:", canonical_count)
    print("Cross-system identities:", cross_system)
    print("Duplicate source links:", duplicate_links)
    print("Reconciled pairs:", pairs.count())

    if links_count != source_count:
        raise ValueError("Source holder loss")

    if duplicate_links:
        raise ValueError("Duplicate source links")

    if canonical_count != source_count - pair_count:
        raise ValueError("Unexpected canonical identity count")

    if cross_system != pair_count:
        raise ValueError("Unexpected cross-system identity count")

    check_expected("Source links", links_count, "EXPECTED_SOURCE_LINKS")
    check_expected(
        "Canonical identities",
        canonical_count,
        "EXPECTED_CANONICAL_IDENTITIES"
    )
    check_expected(
        "Reconciled pairs",
        pair_count,
        "EXPECTED_RECONCILED_PAIRS"
    )

    return canonical, linked


def publish_identity(spark, canonical, linked):
    import os

    if os.environ.get("PUBLISH_IDENTITY") != "YES":
        raise RuntimeError(
            "Publishing requires PUBLISH_IDENTITY=YES"
        )

    namespace = "lakehouse.silver_identity_work"

    canonical_target = (
        namespace + ".holder_canonical_v1"
    )

    links_target = (
        namespace + ".holder_source_link_v1"
    )


    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS {namespace}"
    )

    canonical_count = canonical.count()
    links_count = linked.count()
    cross_system = canonical.where(F.col("source_system_count") == 2).count()
    canonical.writeTo(
        canonical_target
    ).using("iceberg").createOrReplace()

    print("PUBLISHED:", canonical_target)

    linked.writeTo(
        links_target
    ).using("iceberg").createOrReplace()

    print("PUBLISHED:", links_target)

    # Verify persisted data, not only in-memory DataFrames.
    persisted_canonical = spark.table(canonical_target)
    persisted_links = spark.table(links_target)

    if persisted_canonical.count() != canonical_count:
        raise ValueError("Persisted canonical count mismatch")

    if persisted_links.count() != links_count:
        raise ValueError("Persisted link count mismatch")

    if persisted_canonical.where(
        F.col("source_system_count") == 2
    ).count() != cross_system:
        raise ValueError("Persisted cross-system mismatch")

    if persisted_links.select(
        "source_system", "holder_context"
    ).distinct().count() != links_count:
        raise ValueError("Persisted duplicate links")

    print("PASS: PERSISTED IDENTITY TABLES VERIFIED")


def main(spark):
    holders = spark.table("lakehouse.silver.holder")
    clients = spark.table(
        "lakehouse.silver_identity_work.private_client_candidate_v1"
    )

    canonical, linked = build_identity(holders, clients)
    publish_identity(spark, canonical, linked)
    print("\nPASS: IDENTITY RECONCILIATION")


if __name__ == '__main__':
    spark = create_spark()
    try:
        main(spark)
    finally:
        spark.stop()
