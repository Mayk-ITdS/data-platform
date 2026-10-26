package data.enrichment

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row}

object AddressesLookup{

    def generate_address(
    clients:Dataset[Row],
    addresses:Dataset[Row]
        ): Dataset[Row] = {

            val zipCodesCount =(
                addresses
                .select(
                col("ZIP").alias("addresses_zip"),
                col("addresses_count")
                ).dropDuplicates("addresses_zip")
            )
            val clientsWithCount = (
            clients
            .join(zipCodesCount,
            clients("zipCode")===zipCodesCount("addresses_zip"),
            "left"
            ).drop("addresses_zip")
        )

        val clientsWithStreets = (
            clientsWithCount
            .withColumn(
                "street_idx",
                    pmod(
                        xxhash64(
                            col("holder_context"),
                            lit("STREET")
                        ),
                        col("addresses_count")
                    ) + lit(1)
                )
            )
        val addressCandidates = (
             addresses
            .select(
                col("ZIP"),
                col("address_idx"),
                col("FULLNAME"),
                col("FROMHN_NR"),
                col("TOHN_NR")
            )
        )
        val selectedStreet = (
            clientsWithStreets.join(
                addressCandidates,
                clientsWithStreets("zipCode") === addressCandidates("ZIP") &&
                clientsWithStreets("street_idx") === addressCandidates("address_idx"),
                "left"
                )
            )

        val withRanges = (
            selectedStreet
            .withColumn(
                "lower",
                least(
                    col("FROMHN_NR"),
                    col("TOHN_NR")
                )
            ).withColumn(
                "upper",
                greatest(
                    col("FROMHN_NR"),
                    col("TOHN_NR")
                )
            )
        )

        val withHouseNumber = (
            withRanges
            .withColumn(
                "house_number",
                col("lower") +
                    pmod(
                        xxhash64(
                            col("holder_context"),
                            lit("HOUSE")
                         ),
                         col("upper") - col("lower") + lit(1)
                    )
                )
            )
        val withCompleteAddresses = (
            withHouseNumber
            .withColumn(
                "AB_ADDRESS",
                concat_ws(
                    " ",
                    col("house_number").cast("string"),
                    col("FULLNAME")
                )
            ).withColumn(
                "AB_EMAIL",
                concat(
                    lower(col("AB_PRENOM")),
                    lit("."),
                    lower(col("AB_NOM")),
                    lit("@"),
                    lower(col("state_code")),
                    col("house_number").cast("String"),
                    lit(".info")
                )
            )
        )
        withCompleteAddresses
    }


}