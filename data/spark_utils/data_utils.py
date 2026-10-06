from pyspark.sql import functions as F
from pyspark.sql import SparkSession
def stable_hash(columns):
    return F.sha2(
        F.to_json(
            F.struct(*[F.col(c).alias(c) for c in columns]),
            options={"ignoreNullFields": "false"},
        ),
        256,
    )

def create_spark_session(app_name):
    return  (
        SparkSession.builder
            .appName(f"{app_name}")
            .config("spark.sql.repl.eagerEval.enabled", True)
            .config("spark.sql.repl.eagerEval.maxNumRows", 200)
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
    )

def compare_flood_zones(df):
    floods_zones = (
        df.withColumn(
            'floodZonesComparison',
            F.when(
                F.col('ratedFloodZone')== F.col('floodZoneCurrent'),
                "EXACT MATCH"
            )
            .when(
                F.col('ratedFloodZone') != F.col('floodZoneCurrent'),
                "CHANGED"
            )
            .when(
                ((F.col('ratedFloodZone').isNull()) & (F.col('floodZoneCurrent').isNull())),
                "BOTH MISSING"
            )
            .when(
                F.col('ratedFloodZone').isNull(),
                'rated missing'
            )
            .when(
                F.col('floodZoneCurrent').isNull(),
                'current missing'
        ).otherwise("EXCEPTION")
        ).groupBy('floodZonesComparison').agg(
            F.count('*').alias('transaction_rows'),
            F.sum('policyCount').alias('insured_units'),
        ).orderBy('insured_units', 'transaction_rows')
    )
    floods_zones.show(500)
    return floods_zones