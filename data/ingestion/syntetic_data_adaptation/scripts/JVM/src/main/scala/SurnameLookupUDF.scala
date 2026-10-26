import org.apache.spark.sql.SparkSession

object SurnameLookupUDF {

  def register(
      spark: SparkSession,
      thresholds: Array[Long],
      surnames: Array[String]
  ): Unit = {

    spark.udf.register(
      "surname_lookup",
      (ticket: Long) =>
        SurnameLookup.lookup(ticket, thresholds, surnames)
    )
  }
}