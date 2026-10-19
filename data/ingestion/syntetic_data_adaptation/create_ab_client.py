from pyspark.sql import functions as F, Window
from data.census_fips_dict import STATE_FIPS
from data.data_dict import HOLDER_CONTEXT_COLUMNS
from data.ingestion.syntetic_data_adaptation.mapping_dict import create_spark_map, OCCUPANCY_CATEGORY, \
    CONDO_COVERAGE_CATEGORY, BUILDING_CATEGORY, CONDO_POLICY_TEST
from data.spark_utils.data_utils import create_spark_session, stable_hash
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

    fips_state_map = create_spark_map(STATE_FIPS)

    customer_base_df = (policies_df
                        .withColumn('occupancy_category', occupancy_map[F.col('occupancyType')])
                        .withColumn('condo_coverage_category', condo_coverage_map[F.col('condominiumCoverageTypeCode')])
                        .withColumn('building_category', building_map[F.col('buildingDescriptionCode')])
                        .withColumn('coverage_signal',F.coalesce(condo_test_map[F.col('condominiumCoverageTypeCode')],F.lit('UNKNOWN')))
                        .withColumn('holder_context', stable_hash(HOLDER_CONTEXT_COLUMNS)
                                    )
                        )

    clients = customer_base_df.groupBy('holder_context').agg(
        F.first('censusGeoid', ignorenulls=True).alias('censusGeoid'),
        F.first('reportedZipCode', ignorenulls=True).alias('reportedZipCode'),
        F.min('originalNBDate').alias('originalNBDate'),
        F.min('policyEffectiveDate').alias('first_policy_effective'),
        F.max('policyEffectiveDate').alias('last_policy_effective'),
        F.min('policyTerminationDate').alias('first_policy_termination'),
        F.max('policyTerminationDate').alias('last_policy_termination'),
        F.array_sort(F.collect_set('coverage_signal')).alias('coverage_signals'),
        F.array_sort(F.collect_set('condo_coverage_category')).alias('condo_coverage_categories'),
        F.array_sort(F.collect_set('occupancy_category')).alias('occupancy_categories'),
        F.array_sort(F.collect_set('building_category')).alias('building_categories'),
    ).withColumn(
        'holder_type',
        F.when(F.array_contains(
            F.col('coverage_signals'), 'CONDO_ASSOCIATION_SIGNAL'),
            F.lit('Condominium Association')
        ).otherwise(F.lit('PERSON'))
    ).withColumn(
        'state_fips',
                 F.substring(F.col('censusGeoid').cast('string'),1,2)
    ).withColumn(
        'age_at_first_policy',
        F.pmod(F.xxhash64('holder_context',F.lit('AGE')),F.lit(61))+F.lit(20)
    ).withColumn('state_code', fips_state_map[F.col('state_fips')]
    ).withColumn('birth_year',F.year('originalNBDate') - F.col('age_at_first_policy'))

    clients = (
         clients
        .withColumn(
            'sex',
            F.when(
                F.pmod(
                    F.xxhash64(
                        'holder_context',
                        F.lit('SEX')),
                    F.lit(2))==0,
                    F.lit('M')
            ).otherwise(F.lit('F'))
        )
    )
    ssa_window = Window.partitionBy('state','sex','year').orderBy('first_name').rowsBetween(Window.unboundedPreceding,Window.currentRow)
    ssa_total_window = Window.partitionBy('state', 'sex', 'year')

    ssa_ranked = (first_names_df
                  .withColumn('count',F.col('count').cast('long'))
                  .withColumn('ssa_cum_count', F.sum('count').over(ssa_window))
                  .withColumn('ssa_total_count', F.sum('count').over(ssa_total_window))
                  )

    ssa_pool = (
        ssa_ranked.groupBy('state','sex','year')
        .agg(
            F.max('ssa_total_count').alias('total_count'),
            F.array_sort(
                F.collect_list(
                    F.struct('ssa_cum_count','first_name'))
             ).alias('name_pool')
        )
    )
    ssa_pool = (
        ssa_pool
        .withColumnRenamed('state', 'ssa_state')
        .withColumnRenamed('sex', 'ssa_sex')
        .withColumnRenamed('year', 'ssa_year')
    )

    clients = (clients
               .join(ssa_pool,
                     on=(clients.state_code == ssa_pool.ssa_state) &
                        (clients.sex == ssa_pool.ssa_sex) &
                        (clients.birth_year == ssa_pool.ssa_year),
                     how='left'
                     )
               )

    clients = (clients.withColumn('first_name_ticket',
                                 F.pmod(
                                     F.xxhash64(
                                         'holder_context',
                                         F.lit('FIRST_NAME')
                                     ),
                                     F.col('total_count')
                                 )
                            )
               )

    selected_name = F.element_at(
        F.filter(F.col('name_pool'),lambda x: x['ssa_cum_count'] > F.col('first_name_ticket')),
        1
    )

    clients = (clients
               .withColumn('AB_PRENOM',
                           F.when(F.col('holder_type')=='PERSON',
                           selected_name['first_name']
                          ))
               )

    # joined = customer_base_df.join(clients,on='holder_context',how='left')
    poc_clients = (
        clients
        .filter(
            (F.col("holder_type") == "PERSON")
            & F.col("holder_context").isNotNull()
            & F.col("state_code").isNotNull()
            & F.col("sex").isNotNull()
            & F.col("birth_year").isNotNull()
            & F.col("reportedZipCode").isNotNull()
            & F.col("AB_PRENOM").isNotNull()
        )
        .limit(1000)
    )

    poc_clients = poc_clients.select(
        "holder_context",
        "reportedZipCode",
        "state_code",
        "birth_year",
        "sex",
        "first_name"
    )
    last_names_df.show(50)
    poc_clients.show(50)
    return poc_clients


if __name__ == '__main__':

    spark = create_spark_session("Creating Fake Data for our FEMA")
    create_ab_client(spark)