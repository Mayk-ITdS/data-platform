from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StringType, NumericType, BooleanType,
    DateType, TimestampType
)

spark = (
    SparkSession.builder
    .appName("FEMA-PowerBI-Export")
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
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

KEY = "canonical_policy_key"

EXTENSIONS = [
    "premium_financial",
    "building",
    "risks",
    "policy_terms",
    "property",
    "property_usage",
    "policy_rating",
    "holder_classification",
]

ALLOWED_TYPES = (
    StringType, NumericType, BooleanType,
    DateType, TimestampType
)

METADATA = {
    "source_record_json",
    "source_topic",
    "source_partition",
    "source_offset",
    "source_kafka_timestamp",
    "last_cdc_operation",
}

def assert_unique(df, key, label):
    if df.filter(F.col(key).isNull()).limit(1).count():
        raise ValueError(f"NULL key in {label}")

    duplicates = (
        df.groupBy(key)
          .count()
          .filter(F.col("count") > 1)
    )

    if duplicates.limit(1).count():
        raise ValueError(f"Duplicate key in {label}")


try:
    base = spark.table(
        "lakehouse.silver_identity_work.policy_enriched_v1"
    )

    base_columns = [
        c for c in base.columns if c not in METADATA
    ]

    result = base.select(*base_columns)
    expected_count = result.count()

    assert_unique(result, KEY, "policy_enriched_v1")

    print(f"Base policies: {expected_count}")

    for table_name in EXTENSIONS:
        table = spark.table(
            f"lakehouse.silver.{table_name}"
        )

        if KEY not in table.columns:
            raise ValueError(
                f"{table_name} lacks {KEY}; "
                f"available columns: {table.columns}"
            )

        assert_unique(table, KEY, table_name)

        business_columns = [
            field.name
            for field in table.schema.fields
            if (
                field.name != KEY
                and field.name not in METADATA
                and field.name not in {
                    "source_system",
                    "source_primary_key",
                    "source_holder_id",
                    "source_holder_context",
                    "source_policy_unit_id",
                }
                and isinstance(field.dataType, ALLOWED_TYPES)
            )
        ]

        extension = table.select(
            F.col(KEY),
            *[
                F.col(c).alias(f"{table_name}__{c}")
                for c in business_columns
            ]
        ).withColumn(
            f"_matched_{table_name}",
            F.lit(True)
        )

        result = result.join(extension, KEY, "left")

        count_after_join = result.count()

        if count_after_join != expected_count:
            raise AssertionError(
                f"Cardinality violation in {table_name}: "
                f"{expected_count} -> {count_after_join}"
            )

        missing = result.filter(
            F.col(f"_matched_{table_name}").isNull()
        ).count()

        print(
            f"{table_name}: {len(business_columns)} columns, "
            f"{missing} missing policies"
        )

        if missing:
            raise AssertionError(
                f"{table_name} violates required 1:1 coverage"
            )

        result = result.drop(f"_matched_{table_name}")

    print("Final schema:")
    result.printSchema()

    output_dir = Path("/exports")
    temporary_dir = "/exports/_policy_portfolio_tmp"
    final_file = output_dir / "policy_portfolio.parquet"

    result.coalesce(1).write.mode("overwrite").parquet(
        temporary_dir
    )

    parts = list(Path(temporary_dir).glob("part-*.parquet"))

    if len(parts) != 1:
        raise RuntimeError(
            f"Expected one Parquet part, found {len(parts)}"
        )

    parts[0].replace(final_file)

    import shutil
    shutil.rmtree(temporary_dir)

    print(f"PASS: {expected_count} policies exported")
    print(f"FILE: {final_file}")

finally:
    spark.stop()
