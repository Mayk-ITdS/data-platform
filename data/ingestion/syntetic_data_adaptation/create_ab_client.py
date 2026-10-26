from pyspark.sql import functions as F, Window
from data.data_dict import HOLDER_CONTEXT_COLUMNS
from data.ingestion.syntetic_data_adaptation.mapping_dict import create_spark_map, OCCUPANCY_CATEGORY, \
    CONDO_COVERAGE_CATEGORY, BUILDING_CATEGORY, CONDO_POLICY_TEST
from data.ingestion.syntetic_data_adaptation.scripts import prepare_addresses_lookup
from data.ingestion.syntetic_data_adaptation.scripts.prepare_surname_lookup import prepare_surname_lookup
from data.ingestion.syntetic_data_adaptation.scripts.prepare_addresses_lookup import prepare_addresses_lookup
from data.modeling.structuring_functions.build_unit_properties import build_unit_properties_struct
from data.spark_utils.data_utils import create_spark_session, stable_hash
from pathlib import Path
from pyspark.sql import DataFrame

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

    customer_base_df = (
        policies_df
    ).withColumn(
        'occupancy_category',
        occupancy_map[F.col('occupancyType')]
    ).withColumn(
        'condo_coverage_category',
        condo_coverage_map[F.col('condominiumCoverageTypeCode')]
    ).withColumn(
        'building_category',
        building_map[F.col('buildingDescriptionCode')]
    ).withColumn(
        'coverage_signal',
        F.coalesce(
            condo_test_map[F.col('condominiumCoverageTypeCode')],
            F.lit('UNKNOWN'))
    ).withColumn(
        'holder_context',
        stable_hash(HOLDER_CONTEXT_COLUMNS)
    ).withColumn(
        'unit_properties',
        build_unit_properties_struct()
    )

    # clients = customer_base_df.groupBy('holder_context','propertyState').agg(
    #     F.collect_set('unit_properties').alias('unit_properties')
    # )
    #
    # clients.write.mode('overwrite').parquet('data/results/holder_unit_sets')
    # ================================== Write -> Read ============================================
    # units = spark_session.read.parquet('data/results/holder_unit_sets')
    #
    # units = units.select('holder_context','propertyState',F.explode('unit_properties').alias('unit'))
    # holder_units = units.select(
    #     'holder_context',
    #     'propertyState',
    #     'unit.policy_record_id',
    #     'unit.property_location_context',
    #     'unit.holder_classification_context',
    #     'unit.property_usage',
    #     'unit.building_technical_properties',
    #     'unit.building_risk_context',
    #     'unit.policy_contract_context',
    #     'unit.policy_rating_context',
    #     'unit.premium_financial_context',
    # )
    #
    # print("Holder first units columns: ", holder_units.columns)
    # holder_units = holder_units.select(
    #     'holder_context',
    #     'propertyState',
    #     'policy_record_id',
    #     'property_location_context.*',
    #     'holder_classification_context.*',
    #     'property_usage.*',
    #     'building_technical_properties.*',
    #     'building_risk_context.*',
    #     'policy_contract_context.*',
    #     'policy_rating_context.*',
    #     'premium_financial_context.*',
    # )
    #
    # print("Holder second units columns: ",holder_units.columns)
    # holder_units = (
    #     holder_units
    #     .groupBy(
    #         'holder_context',
    #     )
    #     .agg(
    #         F.count('policy_record_id').alias('policy_ids_count'),
    #         F.collect_set("primaryResidenceIndicator").alias("primaryResidenceIndicators"),
    #         F.collect_set("reportedZipCode").alias("reportedZipCodes"),
    #         F.array_distinct(F.collect_set('coverage_signal')).alias('coverage_signals'),
    #         F.sort_array(F.collect_list('policyCount')).alias('policyCounts'),
    #         F.first(
    #             "propertyState",
    #             ignorenulls=True
    #         ).alias("propertyState"),
    #         F.min("policyEffectiveDate").alias("first_policy_effective"),
    #         F.array_sort(
    #             F.collect_list(
    #                 F.struct(
    #                     F.col('policyEffectiveDate'),
    #                     F.col('policyTerminationDate'),
    #                     F.col('policy_record_id'),
    #                     F.col('reportedZipCode'),
    #                     F.col('censusGeoid'),
    #                     F.col('policyCount'),
    #                     F.col('originalConstructionDate'),
    #                     F.col('primaryResidenceIndicator'),
    #                     F.col('building_category'),
    #                     F.col('condo_coverage_category'),
    #                     F.col('coverage_signal'),
    #                     F.col('occupancy_category'),
    #                     F.col('smallBusinessIndicatorBuilding'),
    #                     F.col('houseOfWorshipIndicator'),
    #                     F.col('nonProfitIndicator'),
    #                     F.col('stateOwnedIndicator'),
    #                     F.col('endorsementEffectiveDate'),
    #                     F.col('cancellationDateOfFloodPolicy'),
    #                     F.col('policyTermIndicator'),
    #                     F.col('programTypeIndicator'),
    #
    #                 )
    #             )
    #         ).alias('policy_contracts'),
    #         F.countDistinct('reportedZipCode')
    #         .alias('zip_count'),
    #         F.countDistinct('originalConstructionDate')
    #         .alias('construction_dates_count'),
    #         F.countDistinct('building_category')
    #         .alias('building_category_count'),
    #         F.collect_set('occupancy_category')
    #         .alias('occupancy_categories'),
    #         F.countDistinct('condo_coverage_category')
    #         .alias('condo_coverage_category_count')
    # ).withColumn(
    #     "has_condo_association_signal",
    #     F.array_contains(
    #         F.col("policy_contracts.coverage_signal"),
    #         "CONDO_ASSOCIATION_SIGNAL"
    #     )
    # ).withColumn(
    #     "has_non_profit_signal",
    #     F.array_contains(
    #         F.col("policy_contracts.nonProfitIndicator"),
    #         True
    #     )
    # ).withColumn(
    #     "has_small_business_signal",
    #     F.array_contains(
    #         F.col("policy_contracts.smallBusinessIndicatorBuilding"),
    #         True
    #     )
    # ).withColumn(
    #     "has_religious_signal",
    #     F.array_contains(
    #         F.col("policy_contracts.houseOfWorshipIndicator"),
    #         True
    #     )
    # ).withColumn(
    #     'primary_residence_conflict',
    #         F.size(F.array(
    #             F.transform(
    #                 F.filter(
    #                     F.col('policy_contracts'),
    #                     lambda contract: (contract['primaryResidenceIndicator'] == True)
    #                                      &
    #                                      (contract['reportedZipCode'].isNotNull())
    #                                        ),
    #                 lambda contract: contract['reportedZipCode']
    #             )
    #         )
    #         )>2
    #     )
    # )
    # holders_filtered_single = holder_units.filter(F.array_max(F.col('policyCounts')) == 1)
    # holders_filtered = holder_units.filter(F.array_max(F.col('policyCounts')) > 1)
    # holders_filtered_single.write.mode('overwrite').parquet('data/results/holders_filtered_main.parquet')
    # holders_filtered.write.mode('overwrite').parquet('data/results/holders_filtered_60K.parquet')
    # holders_filtered = spark_session.read.parquet('data/results/holders_filtered_60K.parquet')
    holders_filtered_main = spark_session.read.parquet('data/results/holders_filtered_main.parquet')
    holders_filtered_main.show(10,vertical=True,truncate=False)

    print(f"\nHolder third units columns: , \n{holders_filtered_main.columns}\n")

    holder_types_columns = {
        "has_condo_association_signal": "CONDO_ASSOCIATION",
        "has_non_profit_signal": "NON_PROFIT",
        "has_religious_signal": "RELIGIOUS",
        "has_small_business_signal": "SMALL_BUSINESS",
    }
    clients = (holders_filtered_main
    ).withColumn(
        'holder_type',
        F.coalesce(*[F.when(F.col(col),F.lit(label))
        for col,label in holder_types_columns.items()],
        F.lit('PERSON'))
    ).withColumn(
        'state_code',
        F.col('propertyState')
    ).withColumn(
        'age_at_first_policy',
        F.pmod(F.xxhash64('holder_context',F.lit('AGE')),
               F.lit(61))+F.lit(20)
    )

    private_clients = clients.filter(F.col('holder_type')=='PERSON')

    private_clients.show(50,vertical=True,truncate=False)
    print(f"Private clients count: {private_clients.count()}")

    clients = (
         private_clients
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
    ).withColumn('birth_year',
                 (F.year(F.col('first_policy_effective')) - (F.col('age_at_first_policy').cast('int')))
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

    max_threshold = prepare_surname_lookup(spark_session)
    clients = clients.withColumn(
        'last_name_ticket',
        F.pmod(
            F.xxhash64(
                'holder_context',
                F.lit('last_name')
            ),
            F.lit(max_threshold)
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

    clients = (
        clients
        .withColumn(
            'AB_PRENOM',
            F.when(
                F.col(
                    'holder_type'
                )=='PERSON',
                selected_name['first_name']
            ).otherwise(F.lit('AB_ASSOCIATION'))
        ).withColumn(
            'AB_NOM',
            F.when(
                F.col(
                    'holder_type'
                )=='PERSON',
                F.expr('surname_lookup(last_name_ticket)')
            )
        ).withColumn(
            'zipCode',
            F.element_at(
                F.col(
                    'reportedZipCodes'
                ),1
            )
        )
    )

    tiger_pools = prepare_addresses_lookup(spark_session)

    clients_jdf = (
        spark_session._jvm
        .data.enrichment.AddressesLookup.generate_address(
            clients._jdf, tiger_pools._jdf
        )
    )
    clients = DataFrame(clients_jdf,spark_session)
    clients.select(
        "holder_context",
        "zipCode",
        "AB_PRENOM",
        "AB_NOM",
        "AB_ADDRESS",
        "AB_EMAIL"
    ).show(50, truncate=False)
    output_path = Path('data/results/policies_enriched_v3')
    output_path.mkdir(parents=True,exist_ok=False)
    clients.write.mode('overwrite').parquet(str(output_path))
    clients = spark_session.read.parquet('data/results/policies_enriched_v3')
    joined = customer_base_df.join(clients,on='holder_context',how='left')

    joined.show(50, truncate=False)
    print("Columns of joined",joined.columns)
    return joined

def read_clients(spark):
    clients = spark.read.parquet('data/results/policies_enriched_v3')
    return clients

if __name__ == '__main__':
    spark = create_spark_session('Test session')
    create_ab_client(spark)
