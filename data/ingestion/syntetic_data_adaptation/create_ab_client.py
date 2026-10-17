from pyspark.sql import functions as F
from data.ingestion.syntetic_data_adaptation.mapping_dict import create_spark_map, OCCUPANCY_CATEGORY, \
    CONDO_COVERAGE_CATEGORY, BUILDING_CATEGORY, CONDO_POLICY_TEST
from data.spark_utils.data_utils import create_spark_session
from pathlib import Path

group_columns = [
    'series_signature',
    'geo_group_series',
    'censusGeoid',
    'reportedZipCode',
    'nfipCommunityName',
    'originalConstructionDate',
    'policyEffectiveDate',
    'policyTerminationDate',
    'policyCount',
    'occupancyType',
    'condominiumCoverageTypeCode',
    'buildingDescriptionCode'
]
ROOT = Path(__file__).resolve().parents[3]

def create_ab_client(spark_session):

    policies_raw = spark_session.read.parquet(str(ROOT/'data'/'canonical'/'policies'/'NfipPoliciesV3'))
    policies_df = spark_session.read.parquet(str(ROOT/'data'/'results'/'sql_server_policies_enriched'))
    first_names_df = spark_session.read.parquet(str(ROOT/'data'/'ingestion'/'scraping'/'names'/'ssa_first_names.parquet'))
    last_names_df = spark_session.read.parquet(str(ROOT/'data'/'ingestion'/'scraping'/'names'/'census_last_names.parquet'))

    occupancy_map = create_spark_map(OCCUPANCY_CATEGORY)
    condo_coverage_map = create_spark_map(CONDO_COVERAGE_CATEGORY)
    building_map = create_spark_map(BUILDING_CATEGORY)
    condo_test_map = create_spark_map(CONDO_POLICY_TEST)

    customer_base_df = (policies_df
                        .withColumn('occupancy_category', occupancy_map[F.col('occupancyType')])
                        .withColumn('condo_coverage_category', condo_coverage_map[F.col('condominiumCoverageTypeCode')])
                        .withColumn('building_category', building_map[F.col('buildingDescriptionCode')])
                        .withColumn('coverage_signal',F.coalesce(condo_test_map[F.col('condominiumCoverageTypeCode')],F.lit('UNKNOWN')))
                        )

    policies_test = (customer_base_df.groupBy('series_signature','geo_group_series')
                     .agg(F.count('*').alias('geo_group_row_count'),
                          F.countDistinct('coverage_signal').alias('coverage_distinct_signal_count'),
                          F.array_sort(F.collect_set('coverage_signal')).alias('coverage_distinct_signals'),
                          ).orderBy(F.desc('coverage_distinct_signal_count'),F.desc('geo_group_row_count')
                                    )
                     )

    groups_to_inspect = policies_test.filter(F.col('coverage_distinct_signal_count')>1).select('series_signature','geo_group_series')

    semi_join = customer_base_df.join(groups_to_inspect, on=['series_signature','geo_group_series'], how='semi')

    policies_test_2 = semi_join.groupBy('series_signature','geo_group_series','coverage_signal').agg(
        F.count('*').alias('row_count'),
        F.array_sort(F.collect_set('occupancy_category')).alias('occupancy_categories'),
        F.array_sort(F.collect_set('building_category')).alias('building_categories'),
        F.min(F.col('policyEffectiveDate')).alias('min_policyEffectiveDate'),
        F.max(F.col('policyEffectiveDate')).alias('max_policyEffectiveDate'),
        F.min(F.col('policyTerminationDate')).alias('min_terminationDate'),
        F.max(F.col('policyTerminationDate')).alias('max_terminationDate')

    )
    policies_test.show(300,truncate=False)
    policies_test_2.show(300,truncate=False)

if __name__ == '__main__':

    spark = create_spark_session("Creating Fake Data for our FEMA")
    create_ab_client(spark)