package org

import org.apache.spark.sql.SparkSession
import org.apache.log4j.LogManager

object ApplicationContext {

  val logger = LogManager.getLogger("Start************************************wo")

  var sparkSession: SparkSession =_

  def getSparkSession(applicationName: String, isLocal: Boolean = false): SparkSession = {

    if (isLocal){
      sparkSession = SparkSession.builder
          .master("local[*]")
          .getOrCreate()
    }else {
      sparkSession = SparkSession.builder
          .appName(applicationName)
          .master("local[*]")
          .config("spark.scheduler.mode","FAIR")
          .config("hive.exec.dynamic.partition", "true")
          .config("hive.exec.dynamic.partition.mode" , "nonstrict")
          .enableHiveSupport()
          .getOrCreate()
    }
    sparkSession
  }
}
