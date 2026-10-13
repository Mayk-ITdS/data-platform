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

def compare_safe(df1,df2,columns_series):
    return [df1[col].eqNullSafe(df2[col]) for col in columns_series]

def column_lookup(df, col_name,alias):
    return df.groupBy(*col_name).agg(F.count('*').alias(alias))
def read_rankable_columns():

    spark = start_spark_session("Rankable Columns Reader")
    adjacent = spark.read.parquet(f"data/results/summary_column_ranking")
    single_occurrence = (adjacent.filter(F.col('distinct_values') == 1).orderBy('inspected_group_id'))

    single_occurrence.show(50,truncate=False)

    rankable_only = (adjacent.filter(F.col('distinct_values') > 2).groupBy('inspected','distinct_values'))

    return rankable_only
def small_pandas_scoring():

    pandas_summary = pd.read_csv('results/summary.csv')
    excluded_cols = [
        "inspected_group_id",
        "variant_label",
        "variant_number",
        "variants_in_group",
        "record_sum_in_group",
        "row_count",
        "difference_in"
    ]
    pandas_summary['difference_in'] = None
    for group in pandas_summary['inspected_group_id'].unique():
        current_group = pandas_summary[pandas_summary['inspected_group_id'] == group]
        cols_to_compare = [col for col in pandas_summary.columns if col not in excluded_cols]
        print(f"\nGroup {current_group}")
        for idx,variant_row in current_group.iterrows():
            variant = variant_row['variant_label']
            different_cols = []
            if len(current_group) == 1:
                pandas_summary.at[idx, 'difference_in'] = 'one_variant_group'
                continue
            for col in cols_to_compare:
                col_value = variant_row[col]
                print(f"Variant: {variant}")
                print(f"Column: {col}, Value: {col_value}")
                same_values = current_group[col].eq(col_value) | current_group[col].isna() & pd.isna(col)
                print(f"Same values: {same_values} in {col}")

                if same_values.sum() == 1:
                    different_cols.append(col)
                    print(f"Different column! : {col}")

            pandas_summary.at[idx,'difference_in'] = different_cols
    columns_importance = (pandas_summary[['inspected_group_id', 'difference_in']].explode('difference_in').dropna(subset=['difference_in']).drop_duplicates(['inspected_group_id', 'difference_in'])['difference_in'].value_counts())
    print(columns_importance)
    return columns_importance

def pattern_detection(stage3_rows):

    summary_to_melt = stage3_rows.select('group_signature', 'geo_group_signature', 'variant_signature',
                                         'variants_in_group',
                                         *[F.col(col).cast('string') for col in comparison_columns])
    melt_for_scoring = (
        summary_to_melt
        .melt(
            ids=[
                'group_signature',
                'geo_group_signature',
                'variant_signature',
                'variants_in_group'
            ],
            variableColumnName='column_name',
            values=comparison_columns,
            valueColumnName='value'
        )
    )

    melt_for_scoring.write.mode('overwrite').parquet('data/results/melt_for_scoring.parquet')
    melt_for_scoring = spark_session.read.parquet('data/results/melt_for_scoring.parquet')

    melting_window = Window.partitionBy('geo_group_signature', 'column_name', 'value')
    scored_melts = (
        melt_for_scoring
        .withColumn(
            'value_count',
            F.count('*').over(melting_window))
        .withColumn(
            'discrimination_score',
            F.when(
                F.col('variants_in_group') > 1,
                (F.col('variants_in_group') - F.col('value_count')) /
                (F.col('variants_in_group') - 1)
            )
        )
    )

    spark = start_spark_session('Processing Melted for scoring')

    filtered_non_zeros = scored_melts.filter(F.col('discrimination_score') > 0)

    summary = (filtered_non_zeros.groupBy('geo_group_signature')
               .agg(
        F.count('*').alias('discrimination_column_value_rows'),
        F.countDistinct('column_name').alias('changed_columns_count'),
        F.sort_array(F.collect_set('column_name')).alias('changed_columns'),
        F.max('variants_in_group').alias('n_variants')
    ).withColumn('pattern_count',
                 F.count('*').over(Window.partitionBy(F.col('changed_columns'))))).orderBy('geo_group_signature')

    summary.write.mode('overwrite').parquet('data/results/summary_changing_columns.parquet')
    summary = spark.read.parquet('data/results/summary_changing_columns.parquet')

    pattern_window = (Window.partitionBy('changed_columns')
                      .orderBy(F.rand(seed=42)))

    summary_sample = (summary.withColumn('sample_rank',
                                         F.row_number().over(pattern_window)).filter(F.col('sample_rank') <= 5))
    summary_sample = summary_sample.select('geo_group_signature', 'changed_columns', 'pattern_count', 'sample_rank')
    summary_sample.toPandas().to_csv('data/results/summary_sample.csv', index=False)

    patterns_summary = (
        summary
        .groupBy("changed_columns")
        .agg(
            F.countDistinct("geo_group_signature").alias("groups_count")
        )
        .orderBy(F.desc("groups_count"))
    )

    pattern_groups = summary.select(
        "changed_columns",
        "geo_group_signature"
    )

    patterns_summary.show(100, truncate=False)
    pattern_groups.show(100, truncate=False)

    return pattern_groups

def rollover_policy_test(policies):
    rollover_test_grain = [
        "originalConstructionDate",
        "reportedZipCode",
        "originalNBDate",
        "policyEffectiveDate",
        "policyTerminationDate",
        "censusGeoid",
        "mapPanelNumber",
        "mapPanelSuffix"
    ]

    rollover_value = F.coalesce(
        F.col("rolloverTransferCode"),
        F.lit("__NULL__")
    )

    policies_with_parent = policies.withColumn(
        "parent_signature",
        stable_hash(rollover_test_grain)
    )

    rollover_stats = (
        policies_with_parent
        .groupBy("parent_signature")
        .agg(
            F.count("*").alias("rows_count"),
            F.countDistinct(rollover_value).alias("n_rollover_codes"),
            F.sort_array(
                F.collect_set(rollover_value)
            ).alias("rollover_codes")
        )
    )

    rn_parents = (
        rollover_stats
        .filter(
            F.array_contains(F.col("rollover_codes"), "N") &
            F.array_contains(F.col("rollover_codes"), "R")
        )
        .select("parent_signature")
    )

    rn_rows = (
        policies_with_parent
        .join(
            rn_parents,
            on="parent_signature",
            how="inner"
        )
    )

    rn_shape = (
        rn_rows
        .groupBy("parent_signature")
        .pivot("rolloverTransferCode", ["N", "R"])
        .count()
        .fillna(0)
    )

    (
        rn_shape
        .groupBy("N", "R")
        .count()
        .orderBy(F.desc("count"))
        .show(100, truncate=False)
    )

    return rn_shape