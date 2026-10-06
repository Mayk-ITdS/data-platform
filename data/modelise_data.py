import pyarrow as pa
from pyspark import StorageLevel
from pyspark.sql import SparkSession,Window
from pyspark.sql import functions as F
import pyspark as ps
import logging
import os
import pandas as pd

from spark_utils.data_utils import stable_hash
from spark_utils.data_utils import create_spark_session
import warnings
import seaborn as sns


def column_lookup(df, col_name,alias):

    return df.groupBy(*col_name).agg(F.count('*').alias(alias))

def start_spark_session(name):
    spark = create_spark_session(name)


    warnings.filterwarnings("ignore", category=FutureWarning)

    spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
    spark.conf.set("spark.sql.repl.eagerEval.maxNumRows", 200)
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark
def compare_safe(df1,df2,columns_series):
    return [df1[col].eqNullSafe(df2[col]) for col in columns_series]

def get_dbs():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    logger = logging.getLogger(__name__)

    spark = start_spark_session('Retrieving databases')
    edges_output_path = "data/results/claim_policy_edges"

    claims_full = (
        spark.read.option('recursiveFileLookup', True)
        .parquet('data/canonical/claims')
        .withColumnsRenamed({"id": 'claim_record_id','numberOfFloorsInTheInsuredBuilding':'numberOfFloorsInInsuredBuilding'})
    )
    policies_full = (
        spark.read.option('recursiveFileLookup',True)
        .parquet('data/canonical/policies')
        .withColumnsRenamed({"id":'policy_record_id',"preFIRMConstructionIndicator":'preFIRMIndicator'}).drop('asOfDate')
                     )

    computed_edges = spark.read.parquet(
        edges_output_path
    )
    logger.info('Ended Compute -- Returning Databases')

    return  policies_full,claims_full, computed_edges

def profile_data(policies):
    key_columns = ["originalConstructionDate", 'reportedZipCode', 'rolloverTransferCode', 'originalNBDate',
                   'policyEffectiveDate', 'policyTerminationDate']

    splitting_cols = ['reportedZipCode', 'censusGeoid']

    start_spark_session("Data profiling and modelisation")

    policies_with_signatures = (
                                 policies
                                .withColumn('group_signature',
                                                   stable_hash(key_columns)
                                            )
                                .withColumn('splitting_hash',
                                            stable_hash(splitting_cols)
                                            )
    )

    splitting_hash_analysis = (
        policies_with_signatures
        .groupBy(
            "splitting_hash",
            "reportedZipCode",
            "censusGeoid"
        )
        .agg(
            F.count("*").alias("rows_count"),

            F.countDistinct("group_signature")
            .alias("group_level_count"),

            F.countDistinct("policy_record_id")
            .alias("policy_level_count"),

            F.sort_array(
                F.collect_set("rolloverTransferCode")
            ).alias("rollover_codes"),

            F.countDistinct("originalNBDate")
            .alias("original_nb_dates_count")
        )
    )

    total_rows = (
        splitting_hash_analysis
        .agg(F.sum("rows_count"))
        .first()[0]
    )

    target_70 = total_rows * 0.70

    cumulative_window = Window.orderBy('splitting_hash').rowsBetween(Window.unboundedPreceding, Window.currentRow)

    split_set_final = (
        splitting_hash_analysis
        .withColumn(
            'accumulative_rows_sum',
            F.sum('rows_count').over(cumulative_window)
        )
    ).withColumn('accumulative_rows',
                  F.when(
                      F.col(
                          'accumulative_rows_sum'
                      ) <= target_70,
                      F.lit('SQL_SERVER')
                  ).otherwise(F.lit('ORACLE')))

    split_set_final.show(200,truncate=False)

    # raw_summary.write.mode("overwrite").parquet("data/results/raw_summary_policies")

    # policies_first_grouped = policies_with_signatures.groupBy('group_signature').agg(F.count('*').alias('key_level_group_count'))
    #
    # result_filtered = policies_first_grouped.filter(F.col('key_level_group_count') > 1)
    #
    # stage1_rows = policies_with_signatures.join(result_filtered, on='group_signature', how='inner')
    #
    # second_stage_grain_cols = ["censusGeoid", "mapPanelNumber", "mapPanelSuffix"]
    #
    # second_stage_grouped = stage1_rows.withColumn('geo_group_signature', stable_hash(key_columns+second_stage_grain_cols))
    # geo_grouped = second_stage_grouped.groupBy('geo_group_signature').agg(F.count('*').alias('geo_level_group_count'))
    # geo_grouped_filtered = geo_grouped.filter(F.col('geo_level_group_count') > 1)
    #
    # result_largest_geo = (second_stage_grouped.join(geo_grouped_filtered, on='geo_group_signature', how='inner'))
    # result_largest_geo.write.mode("overwrite").parquet("data/results/first_stage_grain")
    # result_largest_geo = spark.read.parquet("data/results/first_stage_grain")
    #
    #

    # technical_columns = [
    #     "policy_record_id",
    #     "group_signature",
    #     "geo_group_signature",
    #     "key_level_group_count",
    #     "geo_level_group_count",
    #     "variant_signature",
    #     "variants_in_group",
    #     "record_sum_in_group",
    #     "row_count"
    # ]
    #
    # comparison_columns = [col for col in result_largest_geo.columns if col not in technical_columns + key_columns + second_stage_grain_cols]
    #
    # groups_to_inspect = result_largest_geo.withColumn('variant_signature', stable_hash(comparison_columns))
    # print("\n===================================================================================== Printing comparison_columns \n",comparison_columns)
    # variants = (
    #     groups_to_inspect
    #     .groupBy('geo_group_signature','variant_signature',*comparison_columns)
    #     .agg(
    #         F.count("*").alias("row_count")
    #     )
    # )
    #
    # variants_stats = (
    #     variants.groupBy('geo_group_signature')
    #     .agg(
    #         F.count("*").alias("variants_in_group"),
    #         F.sum("row_count").alias("record_sum_in_group")
    #     )
    # )
    #
    # stage3_rows= variants.join(variants_stats, on='geo_group_signature', how='inner')
    #
    # summary_to_melt = stage3_rows.select('geo_group_signature','variant_signature','variants_in_group',
    #                                               *[F.col(col).cast('string') for col in comparison_columns])
    #
    # melt_for_scoring = (
    #     summary_to_melt
    #     .melt(
    #         ids=[
    #             'geo_group_signature',
    #             'variant_signature',
    #             'variants_in_group'
    #              ],
    #         variableColumnName='column_name',
    #         values=comparison_columns,
    #         valueColumnName='value'
    #     )
    # )
    #
    # melt_for_scoring.write.mode('overwrite').parquet('data/results/melt_for_scoring.parquet')
    # melt_for_scoring = spark.read.parquet('data/results/melt_for_scoring.parquet')
    #
    # melting_window = Window.partitionBy('geo_group_signature', 'column_name', 'value')
    # scored_melts = (
    #     melt_for_scoring
    #     .withColumn(
    #         'value_count',
    #         F.count('*').over(melting_window))
    #     .withColumn(
    #         'discrimination_score',
    #         F.when(
    #             F.col('variants_in_group') > 1,
    #             (F.col('variants_in_group') - F.col('value_count')) /
    #             (F.col('variants_in_group') -1)
    #               )
    #     )
    # )
    #
    # spark = start_spark_session('Processing Melted for scoring')
    #
    # filtered_non_zeros = scored_melts.filter(F.col('discrimination_score') > 0)
    #
    # summary = (filtered_non_zeros.groupBy('geo_group_signature')
    #            .agg(
    #     F.count('*').alias('discrimination_column_value_rows'),
    #     F.countDistinct('column_name').alias('changed_columns_count'),
    #     F.sort_array(F.collect_set('column_name')).alias('changed_columns'),
    #     F.max('variants_in_group').alias('n_variants')
    # ).withColumn('pattern_count',
    #              F.count('*').over(Window.partitionBy(F.col('changed_columns'))))).orderBy('geo_group_signature')
    #
    # summary.write.mode('overwrite').parquet('data/results/summary_changing_columns.parquet')
    # summary = spark.read.parquet('data/results/summary_changing_columns.parquet')
    #
    # pattern_window = (Window.partitionBy('changed_columns')
    #                   .orderBy(F.rand(seed=42)))
    #
    # summary_sample = (summary.withColumn('sample_rank',
    #                               F.row_number().over(pattern_window)).filter(F.col('sample_rank') <= 5))
    # summary_sample =  summary_sample.select('geo_group_signature', 'changed_columns', 'pattern_count', 'sample_rank')
    # summary_sample.toPandas().to_csv('data/results/summary_sample.csv', index=False)
    #
    # patterns_summary = (
    #     summary
    #     .groupBy("changed_columns")
    #     .agg(
    #         F.countDistinct("geo_group_signature").alias("groups_count")
    #     )
    #     .orderBy(F.desc("groups_count"))
    # )
    #
    # pattern_groups = summary.select(
    #     "changed_columns",
    #     "geo_group_signature"
    # )
    #
    # patterns_summary.show(100,truncate=False)
    # pattern_groups.show(100,truncate=False)

    return ready_to_split_stats


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

    # 1 row = 1 parent group
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

    # parents containing both N and R
    rn_parents = (
        rollover_stats
        .filter(
            F.array_contains(F.col("rollover_codes"), "N") &
            F.array_contains(F.col("rollover_codes"), "R")
        )
        .select("parent_signature")
    )

    # back to raw rows, but only RN parents
    rn_rows = (
        policies_with_parent
        .join(
            rn_parents,
            on="parent_signature",
            how="inner"
        )
    )

    # count how many N and how many R in each parent
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

def read_rankable_columns():

    spark = start_spark_session("Rankable Columns Reader")
    adjacent = spark.read.parquet(f"data/results/summary_column_ranking")
    single_occurrence = (adjacent.filter(F.col('distinct_values') == 1).orderBy('inspected_group_id'))

    single_occurrence.show(50,truncate=False)

    rankable_only = (adjacent.filter(F.col('distinct_values') > 2).groupBy('inspected','distinct_values'))

if __name__ == '__main__':
    policies, claims, edges = get_dbs()
    # modelisation_of_entities(policies, claims, edges)

    profile_data(policies)
    # rollovered = rollover_policy_test(policies)
