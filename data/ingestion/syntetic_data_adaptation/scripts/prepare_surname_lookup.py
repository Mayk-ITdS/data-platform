from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.window import Window

ROOT = Path(__file__).resolve().parents[4]

def prepare_surname_lookup(spark_session,clients):

    last_names_df = spark_session.read.parquet(str(ROOT/'data'/'ingestion'/'scraping'/'names'/'census_last_names.parquet'))
    census_window = Window.orderBy((F.col('rank_number'))).rowsBetween(Window.unboundedPreceding, Window.currentRow)

    last_names_df = last_names_df.withColumn('rank_number',
                                             F.regexp_replace(
                                                 F.col('Rank'), '^#', ''
                                             ).cast('int')
                                             )

    census_last_names_rank = last_names_df.withColumn(
        'count', F.col('Bearers').cast('long')
    ).withColumn(
        'threshold', F.sum('count').over(census_window)
    )
    census_surnames_set = census_last_names_rank.select('rank_number', 'threshold', 'Last Name')

    total_bearers = (census_surnames_set.agg(F.max('threshold').alias('max_threshold')).first()['max_threshold'])

    return total_bearers,census_last_names_rank