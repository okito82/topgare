package org

import org.apache.log4j.LogManager
import org.BanqueDataFrame._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._

<<<<<<< HEAD
import scala.reflect.macros.whitebox


=======
>>>>>>> origin/master
/**
 * Hello world!
 *
 */
object banque  extends App {

    val logger = LogManager.getLogger("AnalystTopGare*********************************************************")
    val sparkSession = ApplicationContext.getSparkSession("TopGare")


  /* Nom et mel de tous les clients */
  //clientDF.select("nom","email").show()

  /* Date d'attribution sans doublon */
  //portefeuilleDF.select("no_client").show()

  /* Longueur du email des clients (fonction chaine) */
  //clientDF.select(length(col("email"))).show()

<<<<<<< HEAD
  clientDF.withColumn("nn",when(col("no_client") === 1,1).otherwise(0)).show()

=======
  compteDF.show()
>>>>>>> origin/master









}
