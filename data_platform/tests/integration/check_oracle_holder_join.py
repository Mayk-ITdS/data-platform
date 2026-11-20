from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder
    .appName("FEMA-Oracle-Holder-Join-Check")
    .config("spark.sql.catalog.lakehouse",
            "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "hadoop")
    .config("spark.sql.catalog.lakehouse.warehouse",
            "/lakehouse/warehouse")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    h = (
        spark.table("lakehouse.silver.holder")
        .where(F.col("source_system") == "ORACLE")
        .select(
            F.trim("holder_context").alias("holder_context")
        )
    )

    c = (
        spark.table("lakehouse.silver.private_client")
        .where(F.col("source_system") == "ORACLE")
        .select(
            F.trim("holder_context").alias("holder_context"),
            F.col("email")
        )
    )

    print("\n=== ORACLE HOLDER ===")
    print("Holders :", h.count())
    print("Contexts non nuls :",
          h.where(F.col("holder_context").isNotNull()).count())

    print("\n=== ORACLE PRIVATE_CLIENT ===")
    print("Clients :", c.count())
    print("Emails renseignés :",
          c.where(
              F.col("email").isNotNull() &
              (F.trim("email") != "")
          ).count())

    print("\n=== JOINTURE PAR HOLDER_CONTEXT ===")
    joined = h.join(c, "holder_context", "left")

    print("Lignes après jointure :", joined.count())
    print("Emails récupérés :",
          joined.where(F.col("email").isNotNull()).count())

    print("Holders sans correspondance :",
          h.join(c, "holder_context", "left_anti").count())

finally:
    spark.stop()
