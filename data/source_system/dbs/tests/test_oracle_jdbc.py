import os
from pyspark.sql import SparkSession

Spark = SparkSession.builder.appName("OracleAssurePlusTest").getOrCreate()

try:
    df = (
        Spark.read.format("jdbc").options(
            url="jdbc:oracle:thin:@//localhost:1521/FREEPDB1",
            driver="oracle.jdbc.driver.OracleDriver",
            dbtable="ASSUREPLUS.CONNECTION_TEST",
            user="ASSUREPLUS",
            password=os.environ.get("ORACLE_PASSWORD")
        ).load())
    df.printSchema()
    df.show(truncate=False)
finally:
    Spark.stop()
