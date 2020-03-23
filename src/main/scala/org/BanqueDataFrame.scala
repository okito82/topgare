package org

import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.banque.sparkSession

// TODO: modf3 

object BanqueDataFrame {
//TODO modif 44
  val clientSchema = StructType(
    Array(
      StructField("no_client", IntegerType, nullable = true),
      StructField("nom", StringType, nullable = true),
      StructField("email", StringType, nullable = true),
      StructField("commentaire", StringType, nullable = true)
    )
  )

  val clientDF = sparkSession.read.format("csv")
    .option("sep", ",")
    .option("inferSchema", "true")
    .schema(clientSchema)
    .load("src/main/resources/client.csv")

  val comptetSchema = StructType(
    Array(
      StructField("no_compte", IntegerType, nullable = true),
      StructField("solde", IntegerType, nullable = true),
      StructField("no_client", IntegerType, nullable = true)
    )
  )


  val compteDF = sparkSession.read.format("csv")
    .option("sep", ",")
    .option("inferSchema", "true")
    .schema(comptetSchema)
    .load("src/main/resources/compte.csv")

  val commercialSchema = StructType(
    Array(
      StructField("no_commercial", IntegerType, nullable = true),
      StructField("nom", StringType, nullable = true)
    )
  )

  val commercialDF = sparkSession.read.format("csv")
    .option("sep", ",")
    .option("inferSchema", "true")
    .schema(commercialSchema)
    .load("src/main/resources/commercial.csv")

  val portfeuilleSchema = StructType (
    Array(
      StructField("no_commercial", IntegerType, nullable = true),
      StructField("no_client", IntegerType, nullable = true),
      StructField("Date", DateType, nullable = true)
    )
  )

  val portefeuilleDF = sparkSession.read.format("csv")
    .option("sep", ",")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("src/main/resources/portefeuille.csv")


}
