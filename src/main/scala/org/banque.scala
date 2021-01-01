package org

import java.util.Properties
import avro.shaded.com.google.common.collect.Lists
import com.github.sakserv.minicluster.impl.YarnLocalCluster
import javafx.beans.property.Property
import org.TestJackson.{list_column, toMyclass}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager
import org.apache.spark.sql.catalyst.analysis.EliminateView.conf
import org.datanucleus._




object banque  {

   val sparkSession = ApplicationContext.getSparkSession("TopGare")

  def main(args: Array[String]): Unit = {
    //val sparkSession = ApplicationContext.getSparkSession("TopGare")

    val logger = LogManager.getLogger("AnalystTopGare*********************************************************")
    val qq = "$dt"
    val dt = "database"

    case class InfoTable(database: String, nameTable: String, listReadCol: List[String])
    def getInfoTable(nameTable: String): InfoTable={
      val list_col = toMyclass[tools.ColumnList](list_column).getColumnsByTableName(nameTable)
       InfoTable(
         database = list_col.get(s"$qq").mkString("nameTable"),
         nameTable = list_col.get("nameTable").mkString(""),
         listReadCol= list_col.get("listReadCol").mkString("").split(",").toList)
    }
    val InfoTable22 = getInfoTable("table2")
    println(InfoTable22)
   /*
    val list_col = toMyclass[tools.ColumnList](list_column).getColumnsByTableName("table2")
    val tablekarma =  InfoTable(database = list_col.get("database").mkString("nameTable"),nameTable = list_col.get("nameTable").mkString(""),listReadCol= list_col.get("listReadCol").mkString("").split(",").toList)


    */

  //println(tablekarma)

   // clientDF.select(list_col.map(c => col(c)):_*).show()

    val header = "field1:Int,field2:Double,field3:String"

   /* def inferType(field: String) = field.split(":")(1) match {
      case "Int" => IntegerType
      case "Double" => DoubleType
      case "String" => StringType
      case _ => StringType
    }

    val schema = StructType(header.split(",").map(column => StructField(column.split(":")(0), inferType(column), true)))
    schema.printTreeString()

    */
/*

    val url = ClassLoader.getSystemResource("src/main/resources/01/schema.json")
    //val schemaSource = Source.fromFile(url.getFile).getLines.mkString
    val schemaSource = scala.io.Source.fromFile("src/main/resources/01/schema.json").mkString
    val schemaFromJson = DataType.fromJson(schemaSource).asInstanceOf[StructType]
    println(schemaFromJson)
    println(schemaFromJson)
   // val df3 = sparkSession.createDataFrame()//.createDataFrame(
  //    sparkSession.parallelize(structureData),schemaFromJson)
   // df3.printSchema()
    //val sparkSession = ApplicationContext.getSparkSession("TopGare")

    // val logger = LogManager.getLogger("AnalystTopGare*********************************************************")

*/


    import com.github.sakserv.minicluster.impl.HdfsLocalCluster
    val hdfsLocalCluster = new HdfsLocalCluster.Builder()
      .setHdfsNamenodePort(12345)
      .setHdfsNamenodeHttpPort(1111)
      .setHdfsTempDir("embedded_hdfs")
      .setHdfsNumDatanodes(1)
      .setHdfsEnablePermissions(false)
      .setHdfsFormat(true)
      .setHdfsEnableRunningUserAsProxyUser(true)
      .setHdfsConfig(new Configuration())
      .build

    hdfsLocalCluster.start()


    val conf = new Configuration()
    val fs= FileSystem.get(conf)

/*
    conf.set("fs.defaultFS", "hdfs://127.0.0.1:12345")

    fs.create(new Path("/tmp/mySample.txt"))

    println("okok")

 */





    val yarnLocalCluster = new YarnLocalCluster.Builder()
      .setNumNodeManagers(1)
      .setNumLocalDirs(1)
        .setNumLogDirs(1)
          .setResourceManagerAddress("localhost:37001")
          .setResourceManagerHostname("localhost")
          .setResourceManagerSchedulerAddress("localhost:37002")
          .setResourceManagerResourceTrackerAddress("localhost:37003")
          .setResourceManagerWebappAddress("localhost:37004")
          .setUseInJvmContainerExecutor(false)
          .setConfig(new Configuration())
          .build();

    yarnLocalCluster.start();



    import com.github.sakserv.minicluster.impl.ZookeeperLocalCluster
    val zookeeperLocalCluster = new ZookeeperLocalCluster.Builder()
      .setPort(12346)
      .setTempDir("embedded_zookeeper")
      .setZookeeperConnectionString("localhost:12346")
      .setMaxClientCnxns(60)
      .setElectionPort(20001)
      .setQuorumPort(20002)
      .setDeleteDataDirectoryOnClose(false).setServerId(1).setTickTime(2000).build

    zookeeperLocalCluster.start()


/*

    import com.github.sakserv.minicluster.impl.HbaseLocalCluster
    val hbaseLocalCluster = new HbaseLocalCluster.Builder()
      .setHbaseMasterPort(25111)
      .setHbaseMasterInfoPort(-1)
      .setNumRegionServers(1)
      .setHbaseRootDir("embedded_hbase")
      .setZookeeperPort(12346)
      .setZookeeperConnectionString("localhost:12346")
      .setZookeeperZnodeParent("/hbase-unsecure")
      .setHbaseWalReplicationEnabled(false)
      .setHbaseConfiguration(new Configuration())
      .activeRestGateway
      .setHbaseRestHost("localhost")
      .setHbaseRestPort(28000)
      .setHbaseRestReadOnly(false)
      .setHbaseRestThreadMax(100)
      .setHbaseRestThreadMin(2)
      .build
      .build

    hbaseLocalCluster.start()

 */




    //hiveLocalServer2.start()
/*
    import com.github.sakserv.minicluster.impl.HiveLocalMetaStore
    import org.apache.hadoop.hive.conf.HiveConf
    val hiveLocalMetaStore = new HiveLocalMetaStore.Builder()
      .setHiveMetastoreHostname("localhost")
      .setHiveMetastorePort(12347)
      .setHiveMetastoreDerbyDbDir("metastore_db")
      .setHiveScratchDir("hive_scratch_dir")
      .setHiveWarehouseDir("warehouse_dir")
      .setHiveConf(new HiveConf).build

    hiveLocalMetaStore.start()

 */


    import com.github.sakserv.minicluster.impl.KafkaLocalBroker
    val kafkaLocalBroker = new KafkaLocalBroker.Builder()
      .setKafkaHostname("localhost")
      .setKafkaPort(11111)
      .setKafkaBrokerId(1)
      .setKafkaProperties(new Properties)
      .setKafkaTempDir("embedded_kafka")
      .setZookeeperConnectionString("localhost:12346")
      .build

    kafkaLocalBroker.start()




/*
    import com.github.sakserv.minicluster.impl.HiveLocalMetaStore
    import org.apache.hadoop.hive.conf.HiveConf
    val hiveLocalMetaStore = new HiveLocalMetaStore.Builder()
      .setHiveMetastoreHostname("localhost")
      .setHiveMetastorePort(12347)
      .setHiveMetastoreDerbyDbDir("metastore_db")
      .setHiveScratchDir("hive_scratch_dir")
      .setHiveWarehouseDir("warehouse_dir")
      .setHiveConf(new HiveConf).build

    hiveLocalMetaStore.start()
 */


    /*

    import com.github.sakserv.minicluster.impl.HiveLocalServer2
    import org.apache.hadoop.hive.conf.HiveConf
    val hiveLocalServer2 = new HiveLocalServer2.Builder()
      .setHiveServer2Hostname("localhost")
      .setHiveServer2Port(12348)
      .setHiveMetastoreHostname("localhost")
      .setHiveMetastorePort(12347)
      .setHiveMetastoreDerbyDbDir("metastore_db")
      .setHiveScratchDir("hive_scratch_dir")
      .setHiveWarehouseDir("warehouse_dir")
      .setHiveConf(new HiveConf)
      .setZookeeperConnectionString("localhost:12346").build

    hiveLocalServer2.start()

     */






  }

}
