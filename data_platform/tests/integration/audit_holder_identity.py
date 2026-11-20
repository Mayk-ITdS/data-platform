from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("FEMA-Holder-Identity-Audit")
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

try:
    links = spark.table(
        "lakehouse.silver_identity_work.holder_source_link_v1"
    )

    print("\n=== REPARTITION PAR SOURCE ===")
    links.groupBy("source_system").agg(
        F.count("*").alias("holders"),
        F.count("email_normalized").alias("with_email")
    ).show(truncate=False)

    print("\n=== EMAILS COMMUNS AUX DEUX SOURCES ===")
    common = (
        links.where(F.col("email_normalized").isNotNull())
        .groupBy("email_normalized")
        .agg(
            F.countDistinct("source_system").alias("source_count")
        )
        .where(F.col("source_count") > 1)
    )

    print("Emails inter-sources :", common.count())

    print("\n=== IDENTITES CANONIQUES MULTI-SOURCES ===")
    links.groupBy("canonical_holder_id").agg(
        F.countDistinct("source_system").alias("sources")
    ).where("sources > 1").groupBy("sources").count().show()

finally:
    spark.stop()
