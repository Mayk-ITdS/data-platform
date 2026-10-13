import pyarrow as pa
from pyspark import StorageLevel
from pyspark.sql import SparkSession,Window
from pyspark.sql import functions as F
import pyspark as ps
import logging
import os
import pandas as pd
import warnings
import seaborn as sns
from pathlib import Path
from .data_utils import create_spark_session
from data.ingestion.raw_data_ingestion.get_raw_dbs import get_dbs


ROOT = Path(__file__).resolve().parents[2]

def get_sets(sql_server,oracle):

    spark = create_spark_session("DB's Creation")

    sets = {
        'sql_server': spark.read.parquet(sql_server),
        'oracle': spark.read.parquet(oracle),
    }

    return sets

def data_sanity_check(sql_server_df,oracle_df):

    df_server = sql_server_df.select('splitting_hash').distinct()

    df_oracle = oracle_df.select('splitting_hash').distinct()

    overlap_test = df_server.join(df_oracle,on='splitting_hash',how='inner')

    print(f"Splitting hash overlap: {overlap_test.count()}")

def db_equality_test(sql_server_df,oracle_df, policies):
    equal = sql_server_df.count() + oracle_df.count() == policies.count()
    print(f"Equality test: {equal}")

if __name__ == "__main__":

    sets =  get_sets('data/results/sql_server_policies','data/results/oracle_policies')
    data_sanity_check(sets['sql_server'],sets['oracle'])
    policies, _, _ ,spark= get_dbs()
    db_equality_test(sets['sql_server'],sets['oracle'],policies)
    sets['oracle'].show(20,truncate=False,vertical=True)
    sets['sql_server'].show(5,truncate=False)
    sets['sql_server'].printSchema()
    sets['oracle'].printSchema()