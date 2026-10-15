from pyspark.sql import functions as F
from data.spark_utils.data_utils import create_spark_session
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]

def create_ab_client(spark_session):

    policies_df = spark_session.read.parquet(str(ROOT/'data'/'results'/'sql_server_policies_enriched'))

    

    print(policies_df.select('series_signature'))

    return policies_df

if __name__ == '__main__':

    spark = create_spark_session("Creating Fake Data for our FEMA")
    create_ab_client(spark)