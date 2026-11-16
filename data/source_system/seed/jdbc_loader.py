from pyspark.sql import DataFrame
import os
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[3]
load_dotenv(ROOT / ".env")

def write_jdbc(
    df: DataFrame,
    db_url: str,
    user: str,
    password: str,
    table: str,
    driver:str,
    partitions: int = 2,
    batch_size: int = 10_000,

) -> None:
    (
        df.coalesce(partitions)
        .write
        .format("jdbc")
        .option("url", os.environ[db_url])
        .option("dbtable", table)
        .option("user", os.environ[user])
        .option("password", os.environ[password])
        .option(
            "driver",
            driver
        )
        .option("batchsize", str(batch_size))
        .mode("append")
        .save()
    )

def read_jdbc(
        spark,
        db_url: str,
        user: str,
        password: str,
        driver:str,
        table:str
              ):
            return(
                spark.read
                .format("jdbc")
                .option("url", os.environ[db_url])
                .option("dbtable", table)
                .option("user", os.environ.get(user))
                .option("password", os.environ.get(password))
                .option(
                    "driver",
                    driver
            )
        .load()
    )