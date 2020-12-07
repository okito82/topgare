package org

import org.apache.log4j.LogManager
import org.BanqueDataFrame._
import org.TestJackson.{list_column, toMyclass}
import org.apache.spark.sql.functions.col



object banque  {

   val sparkSession = ApplicationContext.getSparkSession("TopGare")

  def main(args: Array[String]): Unit = {
    //val sparkSession = ApplicationContext.getSparkSession("TopGare")

    val logger = LogManager.getLogger("AnalystTopGare*********************************************************")



    import sparkSession.implicits._

    val list_col = toMyclass[tools.ColumnList](list_column).getColumnsByTableName("client2")


    clientDF.select(list_col.map(c => col(c)):_*).show()
  }
  //val sparkSession = ApplicationContext.getSparkSession("TopGare")

   // val logger = LogManager.getLogger("AnalystTopGare*********************************************************")






}
