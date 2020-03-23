package org

import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
<<<<<<< HEAD
import org.apache.spark.sql.SQLImplicits
=======
>>>>>>> origin/master
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions._

// TODO: modif2 
object traitement extends  App {
//TODO modif 44
  val logger = LogManager.getLogger("Traitement**********************************************************************")

 def frequentationGare(/*Path_int_data_frequentation: String,*/ sparkSession: SparkSession, fs: FileSystem): Unit = {
  val dfFrequentation = sparkSession.read.format("csv")
                         .option("sep", ",")
                         .option("inferSchema", "true")
                         .option("header", "true")
                         .load("client.csv")

   dfFrequentation.show()
   println("hello")

  /*dfFrequentation.write.format("csv").save("hdfs://quickstart.cloudera:8020/user/cloudera/bdd/putdata")

 val ClientSchema = StructType(
   Array(
     StructField("id_clieny", StringType, nullable = true)
   )
 )
   val clientdf = sparkSession.sql("select * from banque.client")
   val commercialdf = sparkSession.sql("select * from banque.commercial")
   val comptedf = sparkSession.sql("select * from banque.compte")
   val portefeuilledf = sparkSession.sql("select * from banque.portefeuille")

   portefeuilledf.distinct().show()
   Calendar
   val format = new SimpleDateFormat("dd-MM-yyyy")
   val current_data = format.format(Calendar.getInstance().getTime())

   comptedf.agg(min("solde"))

   def nomprenom(x: String,y: String): String ={
     return x + " " + y
   }
   def udf_merge = udf(nomprenom("o","f"),StringType)








*/
 }

}
