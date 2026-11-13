from data.ingestion.syntetic_data_adaptation.scripts.prepare_addresses_lookup import prepare_addresses_lookup
from pyspark.sql import functions as F, Window, DataFrame
from data.data_dict import HOLDER_CONTEXT_COLUMNS
from data.ingestion.syntetic_data_adaptation.mapping_dict import create_spark_map, OCCUPANCY_CATEGORY, \
    CONDO_COVERAGE_CATEGORY, BUILDING_CATEGORY, CONDO_POLICY_TEST
from data.ingestion.syntetic_data_adaptation.scripts.prepare_surname_lookup import prepare_surname_lookup
from data.modeling.structuring_functions.build_unit_properties import build_unit_properties_struct
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

technical_columns = [
    "ssa_state",
    "ssa_sex",
    "ssa_year",
    "addresses_count",
    "street_idx",
    "ZIP",
    "address_idx",
    "FULLNAME",
    "FROMHN_NR",
    "TOHN_NR",
    "lower",
    "upper",
]


ROOT = Path(__file__).resolve().parents[3]

def create_ab_client(spark_session,db_name  ):

    policies_df = spark_session.read.parquet(str(ROOT/'data'/'results'/f'{db_name}_policies_enriched'))

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

    customer_base_df.write.mode('overwrite').parquet(f'data/results/{db_name}_customer_base')

def join_policies_with_holders(spark_session,db_name):
    policies_with_holders= spark_session.read.parquet(str(ROOT / 'data' / 'results' / f'{db_name}_customer_base'))

    print(f"{"\n"*3}Original grain columns : {policies_with_holders.columns}{"\n"*3}")

    holders_set_v1 = spark_session.read.parquet(
        f"data/results/{db_name}_clients_enriched_with_addresses_and_emails_v3"
    )

    print("MASTER:", spark_session.sparkContext.master)
    print("PARALLELISM:", spark_session.sparkContext.defaultParallelism)
    print(
        "JVM HEAP GiB:",
        spark_session._jvm.java.lang.Runtime.getRuntime().maxMemory() / 1024 ** 3
    )

    policies_with_holders_v1_joined = policies_with_holders.join(holders_set_v1.select(
        "holder_context",
        "holder_type",
        "AB_PRENOM",
        "AB_NOM",
        "AB_ADDRESS",
        "AB_EMAIL",
        "sex",
        "birth_year",
        "state_code",
    ), on='holder_context', how='left')

    policies_with_holders_v1_joined.explain()

    policies_with_holders_v1_joined.printSchema()

    policies_with_holders_v1_joined.write.mode('overwrite').parquet(f'data/results/{db_name}_policies_with_holders_v1')

    return

def create_holders(customer_df,db_name):

    holder_units = (
        customer_df
        .select(
            "holder_context",
            "propertyState",
            "unit_properties.*"
        )
        .select(
            "holder_context",
            "propertyState",
            "policy_record_id",
            "property_location_context.*",
            "holder_classification_context.*",
            "property_usage.*",
            "building_technical_properties.*",
            "building_risk_context.*",
            "policy_contract_context.*",
            "policy_rating_context.*",
            "premium_financial_context.*"
        )
    )

    print("Holder first units columns: ", holder_units.columns)

    print("Holder second units columns: ",holder_units.columns)

    holder_units = (
        holder_units
        .groupBy(
            'holder_context',
        )
        .agg(
            F.count('policy_record_id').alias('policy_ids_count'),
            F.collect_set("primaryResidenceIndicator").alias("primaryResidenceIndicators"),
            F.collect_set("reportedZipCode").alias("reportedZipCodes"),
            F.array_distinct(F.collect_set('coverage_signal')).alias('coverage_signals'),
            F.sort_array(F.collect_list('policyCount')).alias('policyCounts'),
            F.first(
                "propertyState",
                ignorenulls=True
            ).alias("propertyState"),
            F.min("policyEffectiveDate").alias("first_policy_effective"),
            F.array_sort(
                F.collect_list(
                    F.struct(
                        F.col('policyEffectiveDate'),
                        F.col('policyTerminationDate'),
                        F.col('policy_record_id'),
                        F.col('reportedZipCode'),
                        F.col('censusGeoid'),
                        F.col('policyCount'),
                        F.col('originalConstructionDate'),
                        F.col('primaryResidenceIndicator'),
                        F.col('building_category'),
                        F.col('condo_coverage_category'),
                        F.col('coverage_signal'),
                        F.col('occupancy_category'),
                        F.col('smallBusinessIndicatorBuilding'),
                        F.col('houseOfWorshipIndicator'),
                        F.col('nonProfitIndicator'),
                        F.col('stateOwnedIndicator'),
                        F.col('endorsementEffectiveDate'),
                        F.col('cancellationDateOfFloodPolicy'),
                        F.col('policyTermIndicator'),
                        F.col('programTypeIndicator'),

                    )
                )
            ).alias('policy_contracts'),
            F.countDistinct('reportedZipCode')
            .alias('zip_count'),
            F.countDistinct('originalConstructionDate')
            .alias('construction_dates_count'),
            F.countDistinct('building_category')
            .alias('building_category_count'),
            F.collect_set('occupancy_category')
            .alias('occupancy_categories'),
            F.countDistinct('condo_coverage_category')
            .alias('condo_coverage_category_count')
    ).withColumn(
        "has_condo_association_signal",
        F.array_contains(
            F.col("policy_contracts.coverage_signal"),
            "CONDO_ASSOCIATION_SIGNAL"
        )
    ).withColumn(
        "has_non_profit_signal",
        F.array_contains(
            F.col("policy_contracts.nonProfitIndicator"),
            True
        )
    ).withColumn(
        "has_small_business_signal",
        F.array_contains(
            F.col("policy_contracts.smallBusinessIndicatorBuilding"),
            True
        )
    ).withColumn(
        "has_religious_signal",
        F.array_contains(
            F.col("policy_contracts.houseOfWorshipIndicator"),
            True
        )
    ).withColumn(
        'primary_residence_conflict',
            F.size(F.array(
                F.transform(
                    F.filter(
                        F.col('policy_contracts'),
                        lambda contract: (contract['primaryResidenceIndicator'] == True)
                                         &
                                         (contract['reportedZipCode'].isNotNull())
                                           ),
                    lambda contract: contract['reportedZipCode']
                )
            )
            )>2
        )
    )

    holders_filtered_single = holder_units.filter(F.array_max(F.col('policyCounts')) == 1)
    holders_filtered_60K = holder_units.filter(F.array_max(F.col('policyCounts')) > 1)
    holders_filtered_single.write.mode('overwrite').parquet(f'data/results/{db_name}_holders_filtered_main')
    holders_filtered_60K.write.mode('overwrite').parquet(f'data/results/{db_name}_holders_filtered_60K')

    return

def enrich_holders_filtered_single_with_sex_and_birthdate(spark_session,db_name):

    holders_filtered_main = spark_session.read.parquet(f'data/results/{db_name}_holders_filtered_main')

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
    clients.write.mode('overwrite').parquet(f'data/results/{db_name}_holders_filtered_single_with_sex_and_birthdate')
    return clients

def prepare_ssa_pool(spark_session):

    first_names_df = spark_session.read.parquet(
        str(ROOT / 'data' / 'ingestion' / 'scraping' / 'names' / 'ssa_first_names.parquet'))
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
    output_ssa = Path('data/results/ssa_pool')
    ssa_pool.write.mode('overwrite').parquet(str(output_ssa))

    return ssa_pool

def prepare_clients_with_surname_ticket(spark_session,db_name):
    clients = spark_session.read.parquet(f'data/results/{db_name}_holders_filtered_single_with_sex_and_birthdate')
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

    with_addresses_path = Path(f'data/results/{db_name}_clients_with_surname_ticket')
    with_addresses_path.mkdir(parents=True,exist_ok=True)

    clients.write.mode('overwrite').parquet(str(with_addresses_path))
    clients = spark_session.read.parquet(f'data/results/{db_name}_clients_with_surname_ticket')

    clients.printSchema()
    return clients

def join_clients_with_ssa(spark_session,db_name):

    output_ssa = Path('data/results/ssa_pool')
    with_addresses_path = Path(f'data/results/{db_name}_clients_with_surname_ticket')
    clients = spark_session.read.parquet(str(with_addresses_path))
    ssa_pool = spark_session.read.parquet(str(output_ssa))

    clients = (clients
               .join(ssa_pool,
                     on=(clients.state_code == ssa_pool.ssa_state) &
                        (clients.sex == ssa_pool.ssa_sex) &
                        (clients.birth_year == ssa_pool.ssa_year),
                     how='left'
                   )
               )
    clients.explain("formatted")
    clients.printSchema()
    print("Partitions after join:", clients.rdd.getNumPartitions())

    output_clients = Path(f'data/results/{db_name}_clients_with_ssa_v3')
    clients.write.mode('overwrite').parquet(str(output_clients))
    return clients

def enrich_client_with_ssa_names(spark_session,db_name):

    input_clients = Path(f'data/results/{db_name}_clients_with_ssa_v3')
    clients = spark_session.read.parquet(str(input_clients))

    clients = clients.withColumn('first_name_ticket',
                                 F.pmod(
                                     F.xxhash64(
                                         'holder_context',
                                         F.lit('first_name')
                                     ),
                                     F.col('total_count')
                                 )
                            )
    selected_name = F.aggregate(
        F.col("name_pool"),
        F.lit(None).cast("string"),
        lambda acc, x: F.coalesce(
            acc,
            F.when(
                x["ssa_cum_count"] > F.col("first_name_ticket"),
                x["first_name"]
            )
        )
    )

    prepare_surname_lookup(spark_session)
    print(spark_session.catalog.functionExists("surname_lookup"))

    clients = (
        clients
        .withColumn(
            'AB_PRENOM',
            F.when(
                F.col(
                    'holder_type'
                )=='PERSON',
                selected_name
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
    ).drop('name_pool')

    clients.explain("formatted")

    clients_ssa_names = Path(f'data/results/{db_name}_clients_with_ssa_names_v3')
    clients_ssa_names.mkdir(parents=True,exist_ok=True)

    clients.write.mode('overwrite').parquet(str(clients_ssa_names))

    return clients

def fill_null_names_with_john(spark_session,db_name):
    joined = spark_session.read.parquet(f"data/results/{db_name}_clients_with_ssa_names_v3")
    joined = (
        joined
        .withColumn(
            "AB_PRENOM",
            F.when(
                F.col(
                    "AB_PRENOM"
                ).isNull(),
                F.concat(
                    F.lit('John'),
                    F.col('state_code'),
                    F.col('birth_year').cast('string')
                )
            ).otherwise(F.col("AB_PRENOM"))
        )
    )
    print("Null AB_Prenom", joined.filter(F.col("AB_PRENOM").isNull()).count())
    joined.show()
    joined.write.mode("overwrite").parquet(f"data/results/{db_name}_joined_clients_filled_with_john")

    return joined
def enrich_clients_with_addresses(spark_session,db_name):
    clients_with_names = Path(f'data/results/{db_name}_joined_clients_filled_with_john')
    clients = spark_session.read.parquet(str(clients_with_names))

    tiger_pools = prepare_addresses_lookup(spark_session)

    tiger_pools.explain("formatted")
    tiger_output = Path('data/results/tiger_pools')
    tiger_output.mkdir(parents=True,exist_ok=True)

    tiger_pools.write.mode('overwrite').parquet(str(Path(f'data/results/{db_name}_tiger_pools')))
    tiger_pools = spark_session.read.parquet(str(Path(f'data/results/{db_name}_tiger_pools')))

    jvm = spark_session._jvm

    print("SPARK JARS:",
          spark_session.sparkContext.getConf().get("spark.jars", "NONE"))

    print("ADDRESSES LOOKUP TYPE:",
          type(jvm.data.enrichment.AddressesLookup))

    loader = jvm.org.apache.spark.util.Utils.getContextOrSparkClassLoader()

    try:
        cls = loader.loadClass("data.enrichment.AddressesLookup$")
        print("CLASS FOUND:", cls.getName())
    except Exception as e:
        print("CLASS LOAD FAILED:", str(e))
        raise


    clients_jdf = (
        spark_session._jvm
        .data.enrichment.AddressesLookup.generate_address(
            clients._jdf, tiger_pools._jdf
        )
    )
    runtime = spark_session._jvm.java.lang.Runtime.getRuntime()

    print(f"JVM MAX HEAP: {runtime.maxMemory() / 1024 ** 3:.2f} GiB")
    clients_jdf.explain("formatted")

    clients = DataFrame(clients_jdf,spark_session)

    clients.explain("formatted")

    clients.write.mode('overwrite').parquet(str(Path(f'data/results/{db_name}_clients_enriched_with_addresses_and_emails_v3')))

    return clients

def read_clients(spark,db_name):
    clients = spark.read.parquet(f'data/results/{db_name}_policies_enriched_v2')

    return clients

if __name__ == '__main__':
    spark_session = create_spark_session('Create Ab Client v2')
    create_ab_client(spark_session)


