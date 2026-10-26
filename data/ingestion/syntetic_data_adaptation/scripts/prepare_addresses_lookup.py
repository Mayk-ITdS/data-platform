from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from data.spark_utils.data_utils import create_spark_session

ROOT = Path(__file__).resolve().parents[4]

def prepare_addresses_lookup(spark_session):
    addresses_df = (spark_session
                    .read
                    .parquet(str(ROOT/'data'/'ingestion'/'scraping'/'addresses'/'tiger_data')))
    tiger_pool = (
        addresses_df
        .select(
            "ZIP",
            "ARID",
            "FULLNAME",
            "FROMHN",
            "TOHN",
        )
        .dropDuplicates([
            "ZIP",
            "ARID",
            "FULLNAME",
            "FROMHN",
            "TOHN",
        ])
        .filter(
            F.col('FROMHN').rlike(r'^\d+$')
            &
            F.col('TOHN').rlike(r'^\d+$'))
    ).withColumn("FROMHN_NR", F.col("FROMHN").cast("long")
    ).withColumn("TOHN_NR", F.col("TOHN").cast("long")
    ).withColumn("ARID_NR", F.col("ARID").cast("long")
                 )

    tiger_window = (
        Window
        .partitionBy("ZIP")
        .orderBy(
            "ARID_NR",
            "FULLNAME",
            "FROMHN_NR",
            "TOHN_NR",
        )
    )
    zip_window = Window.partitionBy('ZIP')
    tiger_ranked = (
        tiger_pool
        ).withColumn(
            'address_idx',
            F.row_number().over(tiger_window)
        ).withColumn(
            'addresses_count',
            F.count('*').over(zip_window)
        )

    tiger_pools = tiger_ranked.select('FULLNAME','ZIP','address_idx','FROMHN_NR','TOHN_NR','ARID_NR','addresses_count',)
    tiger_pools.show(50,truncate=False)

    return tiger_pools

spark_session = create_spark_session('Addresses Read')
prepare_addresses_lookup(spark_session)
