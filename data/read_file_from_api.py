import requests
import pandas as pd
import re
import py4j
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession,Window
from pyspark.sql import functions as F
import pyspark as ps
import time
from functools import reduce
from operator import and_
from data.spark_utils.data_utils import stable_hash

def read_from_api(top=10000,skip=0,batch_number=1):
    top_batch = f'$top={top}&$skip={skip}'
    batch = requests.request("GET", f'https://www.fema.gov/api/open/v3/NfipClaims?{top_batch}')
    batch = batch.json()['NfipClaims']
    batch = pa.Table.from_pylist(batch)
    print(f'Batch {batch_number} has {len(batch)} rows')
    print(batch)
    if len(batch) == 0:
        return
    pq.write_table(batch, f'claims_batch_{batch_number}.parquet')
    batch_number += 1
    return read_from_api(top,skip=skip+top,batch_number=batch_number)

def loss_constraint(df):
    return (df['policyEffectiveDate'] <= df['dateOfLoss']) & (df['dateOfLoss'] <= df['policyTerminationDate'])

def has_usable_value(col_name):
    value = F.trim(F.col(col_name).cast("string"))

    return (
        F.col(col_name).isNotNull()
        & value.rlike(
            r"(?i)^(?!(?:null|na|currently\s+unavailable|unknown|not\s+applicable)$).+$"
        )
    )
def read_to_compare(db2):

    spark = (SparkSession.builder.master('local[12]')
             .appName("Claims comparison")
             .config('spark.driver.memory','7g')
             .config('spark.executor.memory','7g')
             .config('spark.sql.shuffle.partitions','64')
             .getOrCreate())

    spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
    spark.conf.set("spark.sql.repl.eagerEval.maxNumRows", 200)
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    claims_df1 = (
        spark.read
        .option("recursiveFileLookup", "true")
        .parquet("data/canonical/claims")
    )

    policies_df2 = spark.read.option('recursiveFileLookup','true').parquet(db2)

    candidate_columns = [
    'reportedZipCode',
    "occupancyType",
    "originalNBDate",
    'preFIRMIndicator',
    ]

    claims_df1 = claims_df1.drop('agricultureStructureIndicator','asOfDate')
    policies_df2 = policies_df2.drop('agricultureStructureIndicator','asOfDate')

    claims_hash_base = sorted(['dateOfLoss',
                               'openDate',
                               'netIccPaymentAmount',
                               'netContentsPaymentAmount',
                               "totalBuildingInsuranceCoverage"] + candidate_columns)

    policy_hash_base = sorted(['policyCount',
                               'policyCost',
                               'rateMethod',
                               'totalInsurancePremiumOfThePolicy',
                               'totalBuildingInsuranceCoverage',
                               'federalPolicyFee',
                               'programTypeIndicator',
                               'policyEffectiveDate',
                               'policyTerminationDate',
                               'policyTermIndicator'] + candidate_columns)

    policies_df2 = policies_df2.withColumnsRenamed(
        {'preFIRMConstructionIndicator': 'preFIRMIndicator', 'id': "policy_record_id"})
    claims_df1 = claims_df1.withColumnRenamed('id', 'claim_record_id')

    claims_res = claims_df1.withColumn('claim_signature', stable_hash(claims_hash_base))
    policies_res = policies_df2.withColumn('policy_signature', stable_hash(policy_hash_base))

    candidate_condition = reduce(and_,(has_usable_value(col) for col in candidate_columns),)

    eligible_claims = claims_res.filter(candidate_condition & F.col('claim_record_id').isNotNull() & F.col('dateOfLoss').isNotNull())
    eligible_policies = policies_res.filter(
        candidate_condition
        & F.col("policy_record_id").isNotNull()
        & F.col("policyEffectiveDate").isNotNull()
        & F.col("policyTerminationDate").isNotNull()
        & (F.col("policyEffectiveDate")
                < F.col("policyTerminationDate")
        )
    )

    print("Eligible claims count:", eligible_claims.count())
    print("Eligible policies count:", eligible_policies.count())

    to_match_claims = eligible_claims.select(
        'claim_record_id',
        'claim_signature',
        *candidate_columns,
        'dateOfLoss',
        'openDate',
        F.col('latitude').alias('claim_latitude'),
        F.col('longitude').alias('claim_longitude'),
        F.col('totalBuildingInsuranceCoverage')
        .alias('claim_building_coverage'),
        F.col('originalConstructionDate')
        .alias('claim_construction_date')
    )

    to_match_policies = eligible_policies.select(
        "policy_record_id",
        "policy_signature",
        *candidate_columns,
        "policyEffectiveDate",
        "policyTerminationDate",
        F.col("latitude").alias("policy_latitude"),
        F.col("longitude").alias("policy_longitude"),
        F.col("totalBuildingInsuranceCoverage")
        .alias("policy_building_coverage"),
        F.col("originalConstructionDate")
        .alias("policy_construction_date"),
    )

    print('To match claims:', to_match_claims.count())
    print('To match policies:', to_match_policies.count())

    temporal_condition = (to_match_policies["policyEffectiveDate"] <= to_match_claims['dateOfLoss']) & (to_match_claims['dateOfLoss'] < to_match_policies["policyTerminationDate"])

    valid_pairs = to_match_policies.join(to_match_claims, on=candidate_columns, how="inner").filter(temporal_condition)

    pair_edges = (
        valid_pairs
        .withColumn(
            "candidate_key",
            stable_hash(candidate_columns),
        )
        .withColumn(
            "construction_date_status",
            F.when(
                F.col("policy_construction_date").isNull()
                & F.col("claim_construction_date").isNull(),
                "date_missing_in_both",
            )
            .when(
                F.col("claim_construction_date").isNull(),
                "date_missing_in_claim",
            )
            .when(
                F.col("policy_construction_date").isNull(),
                "date_missing_in_policy",
            )
            .when(
                F.col("claim_construction_date")
                == F.col("policy_construction_date"),
                "equal",
            )
            .otherwise("different"),
        )
        .withColumn(
            "coverage_status",
            F.when(
                F.col("claim_building_coverage")
                == F.col("policy_building_coverage"),
                "equal",
            )
            .when(
                F.col("claim_building_coverage").isNull()
                | F.col("policy_building_coverage").isNull(),
                "missing",
            )
            .otherwise("different"),
        )
        .withColumn(
            "coordinates_status",
            F.when(
                (F.col("claim_latitude") == F.col("policy_latitude"))
                & (F.col("claim_longitude") == F.col("policy_longitude")),
                "equal",
            )
            .when(
                F.col("claim_latitude").isNull()
                | F.col("claim_longitude").isNull()
                | F.col("policy_latitude").isNull()
                | F.col("policy_longitude").isNull(),
                "missing",
            )
            .otherwise("different"),
        )
        .withColumn(
            "pair_key",
            stable_hash([
                "claim_record_id",
                "policy_record_id",
            ]),
        )
        .withColumn(
            "match_rule",
            F.lit("exact_block_and_valid_policy_term"),
        )
        .dropDuplicates([
            "claim_record_id",
            "policy_record_id",
        ])
    )
    edges_output_path = "data/results/claim_policy_edges"

    pair_edges.write.mode("overwrite").parquet(
        edges_output_path
    )

    print("\n--- Pair edges saved successfully ---\n")

    computed_edges = spark.read.parquet(
        edges_output_path
    )

    global_metrics = computed_edges.agg(
        F.count("*").alias("matched_record_pairs"),

        F.countDistinct("claim_record_id")
        .alias("matched_claim_records"),

        F.countDistinct("policy_record_id")
        .alias("matched_policy_records"),

        F.countDistinct("claim_signature")
        .alias("matched_claim_signatures"),

        F.countDistinct("policy_signature")
        .alias("matched_policy_signatures"),

        F.countDistinct("candidate_key")
        .alias("matched_candidate_blocks"),
    )

    metrics = global_metrics.first()

    print(
        "All claim rows:",
        eligible_claims,
    )

    print(
        "All policy rows:",
        eligible_policies,
    )

    print(
        "Matched record pairs:",
        metrics["matched_record_pairs"],
    )

    print(
        "Matched claim records:",
        metrics["matched_claim_records"],
    )

    print(
        "Matched policy records:",
        metrics["matched_policy_records"],
    )

    print(
        "Matched claim signatures:",
        metrics["matched_claim_signatures"],
    )

    print(
        "Matched policy signatures:",
        metrics["matched_policy_signatures"],
    )

    print(
        "Matched candidate blocks:",
        metrics["matched_candidate_blocks"],
    )

    # result_joined_sets_grouped = claim_groups.join(policy_groups,on=candidate_columns, how="inner")
    #
    # policy_date_check = result_joined_sets_grouped.withColumn('policy_date_constraint_check',
    #                                       loss_constraint(result_joined_sets_grouped))
    #
    # policy_date_check = policy_date_check.groupBy('policy_date_constraint_check').agg(F.count("*").alias("check_values_ranking"))
    # policy_date_check.show(truncate=False)
    #
    # claims_join_policies_frequency = result_joined_sets_grouped.groupBy('claim_group_key','policy_group_key').agg(F.count(
    #     "*").alias("pairs_frequency")).orderBy(F.desc("pairs_frequency"))
    #
    # claims_with_how_many_policies = claims_join_policies_frequency.groupBy('claim_group_key').agg(F.count("*").alias("matched_policy_groups")).orderBy(F.desc("matched_policy_groups"))
    # policy_groups_how_many_claims = claims_join_policies_frequency.groupBy('policy_group_key').agg(F.count("*").alias("matched_claim_groups")).orderBy(F.desc("matched_claim_groups"))
    #
    # frequency_of_policies = policy_groups.groupBy('policy_group_key').agg(F.count("*").alias("group_frequency_policies")).orderBy(F.desc("group_frequency_policies"))
    # frequency_of_claims = claim_groups.groupBy('claim_group_key').agg(F.count("*").alias("group_frequency_claims")).orderBy(F.desc("group_frequency_claims"))
    #
    # claims_frequency_window = Window.orderBy(F.desc("group_frequency_claims"))
    # policy_frequency_window = Window.orderBy(F.desc("group_frequency_policies"))
    # uni_frequency_window = Window.orderBy(F.desc('pairs_frequency'))
    #
    # frequency_of_claims_rank = frequency_of_claims.withColumn("group_rank_claims", F.dense_rank().over(claims_frequency_window))
    # frequency_of_policies_rank = frequency_of_policies.withColumn("group_rank_policies", F.dense_rank().over(policy_frequency_window))
    #
    # frequency_joined_sets = claims_join_policies_frequency.withColumn('pair_frequency_rank',F.dense_rank().over(uni_frequency_window))


    return

def add_del_column_recurse(common_columns,candidate_columns):

    new_candidate = common_columns[0]
    common_columns.add(new_candidate)

    return

def normalize_batches(types_dict,data_path,file_name, destination_path):
    from pathlib import Path
    spark = (SparkSession.builder.master('local[2]').appName("Policies normalization").getOrCreate())
    batch_paths = sorted(Path(data_path).glob(file_name), key=lambda path: int(path.stem.rsplit('_',1)[1]))

    for batch_path in batch_paths:
        print(batch_path)
        current = spark.read.parquet(str(batch_path))
        for col, val in types_dict.items():
            if col not in current.columns:
                continue
            if val == 'timestamp':
                current = current.withColumn(col, F.try_to_timestamp(F.col(col),F.lit("yyyy-MM-dd'T'HH:mm:ss.SSSX")))
            elif val == 'date':
                current = current.withColumn(col,F.to_date(F.try_to_timestamp(F.col(col),F.lit("yyyy-MM-dd'T'HH:mm:ss.SSSX"))))
            else:
                current = current.withColumn(col, F.col(col).cast(val))

        output_path = Path(destination_path) / batch_path.stem

        current.write.mode("overwrite").parquet(str(output_path))

def normalize_policies(data_path,destination_path,types_dict):
    from pathlib import Path
    spark = (SparkSession.builder.master('local[2]').appName("Policies normalization").getOrCreate())
    data_path = Path(data_path)
    if not data_path.is_file():
        raise FileNotFoundError(f"The path {data_path} is not a file")

    policies_df = spark.read.parquet(str(data_path))

    for col, val in types_dict.items():
        if col not in policies_df.columns:
            continue
        if val == 'timestamp':
            policies_df = policies_df.withColumn(col, F.try_to_timestamp(F.col(col),F.lit("yyyy-MM-dd'T'HH:mm:ss.SSSX")))
        elif val == 'date':
            policies_df = policies_df.withColumn(col,F.to_date(F.try_to_timestamp(F.col(col),F.lit("yyyy-MM-dd'T'HH:mm:ss.SSSX"))))
        else:
            policies_df = policies_df.withColumn(col, F.col(col).cast(val))
    policies_df.write.mode("overwrite").parquet(str(destination_path))


def test_read_batches():
    spark = (SparkSession.builder.master('local[2]').appName("Claims comparison").getOrCreate())
    claims_df = spark.read.parquet("data/canonical/claims_batch_*")
    claims_df.printSchema()
    print(claims_df.count())

if __name__ == '__main__':
    read_to_compare('data/canonical/policies')