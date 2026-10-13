from pyspark.sql import functions as F
from data.data_dict import key_columns, GEO_GROUP_COLUMNS
from data.spark_utils.data_utils import create_spark_session,stable_hash
from data.modeling.add_series_signature import add_series_signature
def main():
    spark = create_spark_session("Policy series modeling")

    sql_server = (
        spark.read.parquet("data/results/sql_server_policies")
        .withColumn("source_system", F.lit("SQL_SERVER"))
    )

    oracle = (
        spark.read.parquet("data/results/oracle_policies")
        .withColumn("source_system", F.lit("ORACLE"))
    )

    sql_server = add_series_signature(sql_server,'series_signature',key_columns)
    oracle = add_series_signature(oracle,'series_signature',key_columns)
    sql_server = add_series_signature(sql_server,'geo_group_series',GEO_GROUP_COLUMNS)
    oracle = add_series_signature(oracle,'geo_group_series',GEO_GROUP_COLUMNS)

    print(
        "JVM max heap GB:",
        spark._jvm.java.lang.Runtime.getRuntime().maxMemory() / 1024 ** 3
    )

    series_overlap = sql_server.select('series_signature','geo_group_series').distinct().join(
        oracle.select('series_signature','geo_group_series').distinct(),on=['series_signature','geo_group_series'],how='inner'
    )
    print(f'Series overlap: {series_overlap.count()}')

    signatures_grouped = sql_server.groupBy("series_signature","geo_group_series",).agg(
        F.count("*").alias("policy_records_count"),
    )

    series_counts = signatures_grouped.agg(
        F.count("*").alias("geo_group_count"),
        F.avg('policy_records_count').alias("avg_rows_per_geo_group"),
        F.max('policy_records_count').alias("max_rows_per_geo_group"),
    )

    sql_server.write.mode("overwrite").parquet(
        "data/results/sql_server_policies_enriched"
    )

    oracle.write.mode("overwrite").parquet(
        "data/results/oracle_policies_enriched"
    )

    signatures_grouped.orderBy(F.desc('policy_records_count')).show(20,truncate=False)
    series_counts.show()


if __name__ == "__main__":
   main()