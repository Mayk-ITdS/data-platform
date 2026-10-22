from pyspark.sql import functions as F, Window
from data.data_dict import HOLDER_CONTEXT_COLUMNS
from data.ingestion.syntetic_data_adaptation.mapping_dict import create_spark_map, OCCUPANCY_CATEGORY, \
    CONDO_COVERAGE_CATEGORY, BUILDING_CATEGORY, CONDO_POLICY_TEST
from data.ingestion.syntetic_data_adaptation.scripts.prepare_surname_lookup import prepare_surname_lookup
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


    policies_df = spark_session.read.parquet(str(ROOT/'data'/'results'/'sql_server_policies_enriched'))
    first_names_df = spark_session.read.parquet(str(ROOT/'data'/'ingestion'/'scraping'/'names'/'ssa_first_names.parquet'))

    occupancy_map = create_spark_map(OCCUPANCY_CATEGORY)
    condo_coverage_map = create_spark_map(CONDO_COVERAGE_CATEGORY)
    building_map = create_spark_map(BUILDING_CATEGORY)
    condo_test_map = create_spark_map(CONDO_POLICY_TEST)

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
        F.min_by(F.col('propertyState'),F.col('originalNBDate')).alias('propertyState'),
        F.sort_array(F.collect_set(F.col('propertyState'))).alias('propertyStates'),
        F.array_sort(F.collect_set(F.col('reportedZipCode'))).alias('reportedZipCode'),
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
        'state_code',
        F.col('propertyState')
    ).withColumn(
        'age_at_first_policy',
        F.pmod(F.xxhash64('holder_context',F.lit('AGE')),
               F.lit(61))+F.lit(20)
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
    ssa_window = Window.partitionBy(
        'state',
        'sex',
        'year'
    ).orderBy(
        'first_name'
    ).rowsBetween(Window.unboundedPreceding,Window.currentRow)

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

    total_bearers, census_surnames_rank = prepare_surname_lookup(spark_session,clients)

    clients = clients.withColumn(
        'last_name_ticket',
        F.pmod(
            F.xxhash64(
                'holder_context',
                F.lit('last_name')
            ),
            F.lit(total_bearers)
        )
    )
    clients = (clients
               .join(ssa_pool,
                     on=(clients.state_code == ssa_pool.ssa_state) &
                        (clients.sex == ssa_pool.ssa_sex) &
                        (clients.birth_year == ssa_pool.ssa_year),
                     how='left'
                   )
               )

    clients = clients.withColumn('first_name_ticket',
                                 F.pmod(
                                     F.xxhash64(
                                         'holder_context',
                                         F.lit('first_name')
                                     ),
                                     F.col('total_count')
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
                          ).otherwise(F.lit('AB_ASSOCIATION')))
               )

    clients.show(50, truncate=False, vertical=True)

    # joined = customer_base_df.join(clients,on='holder_context',how='left')


    return clients


if __name__ == '__main__':
    spark = create_spark_session('Test session')
    create_ab_client(spark)