from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.window import Window

ROOT = Path(__file__).resolve().parents[4]

def prepare_surname_lookup(spark_session):

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
    census_surnames_set = (census_last_names_rank
                           .select('threshold',
                                   'Last Name'
                                   )
                           )
    census_surnames_set = census_surnames_set.orderBy('threshold').collect()

    max_threshold = census_surnames_set[-1][0]
    thresholds = [row['threshold'] for row in census_surnames_set]
    surnames = [row['Last Name'] for row in census_surnames_set]

    gateway = spark_session.sparkContext._gateway
    jvm = spark_session._jvm

    jvm_thresholds = gateway.new_array(
        jvm.long,
        len(thresholds)
    )
    for i, value in enumerate(thresholds):
        jvm_thresholds[i] = value

    jvm_surnames = gateway.new_array(
        jvm.java.lang.String,
        len(surnames)
    )
    for i, value in enumerate(surnames):
        jvm_surnames[i] = value

    jvm.SurnameLookupUDF.register(
        spark_session._jsparkSession,
        jvm_thresholds,
        jvm_surnames
    )

    return max_threshold