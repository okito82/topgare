package org.tools
/*

import java.io.{File, IOException}
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import scala.collection.mutable.LinkedHashMap
import scala.reflect.ClassTag
import scala.util.Try

object BasicFunction {

  protected val logger = Logger.getLogger(this.getClass.getName)


  def getConfCluster(mode: String,couloir:String): (ConfigCluster, ColumnList) = {
    val master=mode.toLowerCase
    val confCluster = fromResources(s"config/master/newModel/config_${couloir}_${master}.json").get
    val list_columns = fromResources(s"config/hive/newModel/list_selectedColumns.json").get
    val cfCluster= JsonMapper.toMyClass[ConfigCluster](confCluster)
    if (master=="local")
      cfCluster.rootPath = "file:/" + new File(cfCluster.rootPath).getAbsoluteFile.toString.replace("\\", "/")
    (cfCluster, JsonMapper.toMyClass[ColumnList](list_columns))
  }

  /**
   * Get just one argument
   *
   * @param args
   * @param argument
   * @return
   */
  def getArgument(args: Array[String], argument: String): String = {
    args.filter(_.contains(argument))(0).split("=")(1) //.toLowerCase
  }

  /**
   * Get All parameters and dt_closing
   *
   * @param args parameters Job
   * @return
   */
  def getArgumentsWithDtClosing(args: Array[String]): (JobArgumentGlobal, String) = {
    (getJobArgumentsGlobal(args), getArgument(args, "dtClosing"))
  }

  /**
   * Parameters
   *
   * @param args
   * @return
   */
  def getJobArgumentsGlobal(args: Array[String]): JobArgumentGlobal = {
    JobArgumentGlobal(
      BasicFunction.getArgument(args, "master"), // local ou yarn
      BasicFunction.getArgument(args, "rootPath"), //hdfsId:port
      BasicFunction.getArgument(args, "hdfsEnvName"), //environment : dev/rt/rec/ppd/prd
      BasicFunction.getArgument(args, "dataRootPath"), //when we use config from HDFS
      Try(BasicFunction.getArgument(args, "verRef")).toOption) //Referential version (actually V3)
  }


  /**
   * Get new ConfigCluster, when we use configCluster embedded in Jar
   *
   * @param configCluster configCluster initial
   * @param envVariable parameters job recover from GCLWEB (like mainJob.properties file)
   * @return
   */
  def setConfigCluster(configCluster: ConfigCluster)(implicit envVariable: JobArgumentGlobal): ConfigCluster = {

    val rootPath = envVariable.rootPath
    val hdfsEnvName = envVariable.hdfsEnvName
    val dataRootPath = s"${envVariable.dataRootPath}/$hdfsEnvName"
    val refVer = envVariable.refVersion.getOrElse("").toUpperCase

    //specific to perimeter project
    val outputRefConfig = if (refVer.equals("")) "" else s"$dataRootPath/perimeter/$refVer/${configCluster.outputRefConfig}"
    //specific to perimeter project
    val outputRefCsv = if (refVer.equals("")) "" else s"$dataRootPath/perimeter/$refVer/${configCluster.outputRefCsv}"

    def setBaseHive(nameBase: String): String = {
      val fraLayer = nameBase.split("_").drop(1).mkString("", "_", "")
      s"${hdfsEnvName}_$fraLayer"
    }

    val inputSchemaRef = configCluster.inputRefSchema //specific to perimeter project
    val inputData = configCluster.getInputTables map {
      table =>
        table._1 -> HiveTable(s"$dataRootPath/${table._2.pathParquet}", setBaseHive(table._2.baseHive), table._2.nameTable)
    }

    val outputData = configCluster.getOutputTables map {
      table =>
        table._1 -> HiveTable(s"$dataRootPath/${table._2.pathParquet}", setBaseHive(table._2.baseHive), table._2.nameTable)
    }

    ConfigCluster(rootPath, outputRefCsv, outputRefConfig, inputSchemaRef, inputData, outputData)
  }

  /**
   * Useful when the file configuration are in HDFS
   *
   * @param pathBaseHDFS
   * @param outputTableName
   * @param sparkSession
   * @param runMode
   * @param configLoadMode
   * @return
   */
  def loadConfigCluster(outputTableName: String, pathBaseHDFS: Option[String], list_columns: Option[String] = None)(implicit sparkSession: SparkSession, jobArgumentGlobal: JobArgumentGlobal, runMode: RunMasterMode, configLoadMode: ConfigLoadMode): (ConfigCluster, ColumnList, SheetSchema) = {

    (runMode, configLoadMode) match {
      case (ClusterRun, ClusterConfigMode) => //Run in Yarn cluster and load config from cluster
        getConfigFromHdfs(pathBaseHDFS.get, ClusterRun.name, outputTableName: String, list_columns: Option[String])

      case (ClusterRun, LocalConfigMode) => //Run in Yarn cluster and load config from local resources
      {
        val (configCluster, columnList, sheetSchema) = BasicFunction.getConfigFromLocal(ClusterRun.name, outputTableName, list_columns)
        (BasicFunction.setConfigCluster(configCluster), columnList, sheetSchema)
      }
      case (_, _) => //Run in local standalone and load config from local resources
        getConfigFromLocal(runMode.name, outputTableName: String, list_columns: Option[String])
    }
  }

  /**
   * Get configCluster, columns list to be treated and HiveTable from HDFS
   *
   * @param pathBaseHDFS
   * @param master
   * @param outputTableName
   * @param listColumns
   * @param sparkSession
   * @return
   */

  def getConfigFromHdfs(pathBaseHDFS: String, master: String, outputTableName: String, listColumns: Option[String])(implicit sparkSession: SparkSession): (ConfigCluster, ColumnList, SheetSchema) = {
    val (confCluster, columnsList, nameTable) =
      (fromFile(s"$pathBaseHDFS/config/master/config_yarn.json"),
        fromFile(s"$pathBaseHDFS/config/hive/${listColumns.getOrElse("list_columns")}.json"),
        fromFile(s"$pathBaseHDFS/config/hive/$outputTableName.json"))

    (JsonMapper.toMyClass[ConfigCluster](confCluster),
      JsonMapper.toMyClass[ColumnList](columnsList),
      JsonMapper.toMyClass[SheetSchema](nameTable))
  }

  /**
   * Get configCluster, columns list to be treated and HiveTable from Local(Embedded in my Jar)
   *
   * @param mode
   * @param outputTableName
   * @param listColumns
   * @param sparkSession
   * @return
   */
  def getConfigFromLocal(mode: String, outputTableName: String, listColumns: Option[String])(implicit sparkSession: SparkSession): (ConfigCluster, ColumnList, SheetSchema) = {

    val (confCluster, columnsList, nameTable) =
      (fromResources(s"config/master/config_${mode.toLowerCase}.json").get,
        fromResources(s"config/hive/${listColumns.getOrElse("list_columns")}.json").get,
        fromResources(s"config/hive/$outputTableName.json").get)

    (JsonMapper.toMyClass[ConfigCluster](confCluster),
      JsonMapper.toMyClass[ColumnList](columnsList),
      JsonMapper.toMyClass[SheetSchema](nameTable))

  }


  /**
   * It's define from where could we get our configs
   *
   * @param args paramters Job
   * @return Hdfs/Local
   */
  def getConfigLoadMode(args: Array[String]): ConfigLoadMode = {
    val mode = Try(getArgument(args, "configLoadMode"))

    (mode.isSuccess, mode.getOrElse("local")) match {
      case (true, "hdfs") => ClusterConfigMode
      case (_, _) => LocalConfigMode
    }
  }

  /**
   * get environment to be excused our job
   *
   * @param jobArgumentGlobal parameters Job
   * @return Standalone/Yarn
   */
  def getRunMasterMode(implicit jobArgumentGlobal: JobArgumentGlobal): RunMasterMode = {
    jobArgumentGlobal.master match {
      case "local" => LocalRun
      case _ => ClusterRun
    }
  }

  /**
   * get environment to be excused our job
   *
   * @param args parameters Job
   * @return Standalone/Yarn
   */
  def getRunMasterMode(args: Array[String]): RunMasterMode = {
    Try(getArgument(args, "master")).getOrElse("yarn") match {
      case "local" => LocalRun
      case _ => ClusterRun
    }
  }


  /**
   * Allows renaming column of dataSet
   *
   * @param dsInput
   * @param mapRenamedColumns
   * @return
   */
  def renamingColumns(dsInput: Dataset[Row], mapRenamedColumns: LinkedHashMap[String, String]): Dataset[Row] = {
    if (mapRenamedColumns.nonEmpty)
      mapRenamedColumns.keys.foldLeft(dsInput) { (ds, nameCol) => ds.withColumnRenamed(nameCol, mapRenamedColumns(nameCol)) }
    else
      dsInput
  }

  /**
   * getArgument of spark job
   *
   * @param args
   * @return
   */
  @deprecated("replaced by getJobArgumentGlobal function", "1.2")
  def getArgument(args: Array[String]): Argument = {

    Argument(args.filter(_.contains("ver"))(0).split("=")(1).toUpperCase,
      args.filter(_.contains("master"))(0).split("=")(1),
      args.filter(_.contains("dt_closing"))(0).split("=")(1),
      Try {
        args.filter(_.contains("path_config"))(0).split("=")(1).trim.toLowerCase
      })

  }

  /**
   * Read file from Resource Jar
   *
   * @param fileName
   * @throws java.io.IOException
   * @return
   */
  @throws(classOf[IOException])
  def fromResources(fileName: String): Try[String] = {

    val fileURL = getClass.getClassLoader.getResource(fileName)
    if (fileURL == null) {
      logger.error(s"resources '${fileName}' doesn't exist")
      System.exit(1)
    }
    val file = fileURL.openStream()
    Try {
      scala.io.Source.fromInputStream(file).getLines.mkString("\n")
    }
  }

  /**
   * get Schema Table who exists in configCluster and loaded since Json File in HDFS/LOCAL
   *
   * @param tableName
   * @param sparkSession
   * @return
   */
  def getSchemaTable(tableName: Option[String] = None)(implicit sparkSession: SparkSession, configCluster: ConfigCluster, arguments: JobArgumentGlobal): SheetSchema = {

    val nameTable = fromResources(s"config/hive/${tableName.get}.json").get

    JsonMapper.toMyClass[SheetSchema](nameTable)
  }


  /**
   * Check if a validate date,  in the format YYYY-MM-DD
   *
   * @param date
   * @return
   */
  def isDate(date: String): Boolean = {
    getDate(date) match {
      case None => false
      case _ => true
    }
  }

  /**
   * Convert fromat date : DD-MM-YYYY to YYYY-MM-DD
   *
   * @param date
   * @return option[String]
   */
  def getDate(date: String): Option[String] = {
    val regex1 = "(\\d{4}-\\d{2}-\\d{2})".r
    val regex2 = "(\\d{2}/\\d{2}/\\d{4})".r

    date match {
      case regex1(date) => Some(date)
      case regex2(date) => val arr = date.split("/"); Some(s"${arr.last}-${arr(1)}-${arr.head}")
      case _ => None
    }
  }

  /**
   * Get a Loan case of Perimeter engine by retry mode
   *
   * @param mapTables input table
   * @param dtClosing date d'arrete used to run Perimeter
   * @param session SparkSession
   * @param columnList column list to be treated and used to calculate Top_*
   * @return Dataset[Row]
   */
  def getLoanCaseByRetry(mapTables: Map[String, Dataset[Row]], dtClosing: String)(implicit session: SparkSession, columnList: ColumnList): Dataset[Row] = {

    println("\t\tGet LoanCase parquet without dt_valid_end and dt_closing : \n")

    // t_concept_relation filtered
    val joinedKeyCtr = columnList.getColumnsByTableName("key-join-ctr-rel") ++
      columnList.getColumnsByTableName("key-join-rel-clt") ++
      columnList.getColumnsByTableName("table-relation")

    val dsRelation: Dataset[Row] = mapTables("t_concept_rel_ctr_clt")
      .select(joinedKeyCtr.map(col => mapTables("t_concept_rel_ctr_clt")(col)): _*)

    // relation by rename columns
    val renamedDsRelation = renamingColumns(
      dsRelation.transform(getDsByRetryOfRelation(Seq("id_ctr", "id_clt"),Seq("dt_valid_start", "dt_valid_end"), dtClosing)),
      columnList.getRenamedColumnByTableName("table-relation"))

    // t_concept_contract filtered
    val dsContract: Dataset[Row] = mapTables("t_concept_contract")


    //contract by rename columns
    val renamedDsContract = renamingColumns(
      dsContract.transform(getDsByRetry(Seq("id_ctr","dt_valid_start", "dt_valid_end"),Seq("dt_valid_start", "dt_valid_end"), dtClosing)),
      columnList.getRenamedColumnByTableName("table-contract"))

    // t_concept_client filtered
    val joinedKeyCtl = columnList.getColumnsByTableName("table-client") ++ columnList.getColumnsByTableName("key-join-rel-clt")

    val dsClient: Dataset[Row] = mapTables("t_concept_client")
      .select(joinedKeyCtl.map(col => mapTables("t_concept_client")(col)): _*)

    // client by rename columns
    val renamedDsClient = renamingColumns(
      dsClient.transform(getDsByRetry(Seq("id_clt","dt_valid_start", "dt_valid_end"), Seq("dt_valid_start", "dt_valid_end"), dtClosing)),
      columnList.getRenamedColumnByTableName("table-client"))


    renamedDsContract
      .join(renamedDsRelation, columnList.getColumnsByTableName("key-join-ctr-rel"), "inner") //Join contract table and  relation table
      .join(renamedDsClient, columnList.getColumnsByTableName("key-join-rel-clt"), "inner") //join result with client table

  }


  /**
   * DataSet Normalized
   *
   * @param dsParquet
   * @param session
   * @return
   */
  def normalizedDs(dsParquet: Dataset[Row])(implicit session: SparkSession): Dataset[Row] = {
    import session.sqlContext.implicits._

    val dsParquetNormalized = dsParquet
      .withColumn("cd_customer_sex", Udfs.udfToCustomerSexe($"cd_title", $"cd_clt_type"))
      .withColumn("cd_app_orig", trim($"cd_app_orig"))
      .withColumn("id_facility", Udfs.refToFacility($"ref_ctr_src", $"cd_app_orig", $"ref_ctr_orig"))
      //.withColumn("id_contract", Udfs.getIdContract($"ref_ctr_princ", $"ref_ctr_src", $"cd_app_src", $"cd_app_orig", $"ref_ctr_orig"))
      .withColumn("id_clt_local", $"ref_clt_src") // Bug 1.8.16 : lpad1601($"ref_clt_src", 11, "0")
      .withColumn("cd_type_prod", trim($"cd_type_prod"))
      .withColumn("cd_base_prod", trim($"cd_base_prod"))
      .withColumn("id_clt_group", Udfs.getIdCLtGrp($"cd_clt_type", $"ref_clt_group_risk", $"ref_nat_id"))

    dsParquetNormalized
  }

  /**
   * Filter Data set and get active row
   *
   * @param refDs
   * @param dtClosing
   * @return
   */
  def filterDate(refDs: Dataset[Row], dtClosing: String): Dataset[Row] = {

    refDs.filter(
      col("dt_valid_start") <= to_date(lit(dtClosing)) && (
        col("dt_valid_end") > to_date(lit(dtClosing)) || col("dt_valid_end").isNull) //bug : >=
    )
  }


  /**
   * allows to recover one row by groupBy and sort them
   * Actually, used by contract and client
   *
   * @param columnToPartitionBy used by groupBy
   * @param columnToOrderBy used to sort
   * @param dtClosing
   * @param dS Dateset[Row] input
   * @return Dataset[Row] output
   */
  def getDsByRetry(columnToPartitionBy: Seq[String], columnToOrderBy: Seq[String], dtClosing: String)(dS: Dataset[Row]): Dataset[Row] = {

    val window = Window.partitionBy(columnToPartitionBy.head, columnToPartitionBy.tail: _*).orderBy(columnToOrderBy.head,columnToOrderBy.tail: _*)

    dS.transform(filterByRetry(dtClosing)).
      withColumn("rn", row_number() over window ).where(col("rn") === 1).drop("rn")//.withColumn(dtValidEnd, to_date(lit("9999-12-31")))

  }

  /**
   * allows to recover one row by groupBy and sort them
   * Actually, used by relation table (t_concept_rel_ctr_clt)
   *
   * @param columnToPartitionBy used by groupBy
   * @param columnToOrderBy used to sort
   * @param dtClosing
   * @param dS Dateset[Row] input
   * @return Dataset[Row] output
   */
  def getDsByRetryOfRelation(columnToPartitionBy: Seq[String], columnToOrderBy: Seq[String], dtClosing: String)(dS: Dataset[Row]): Dataset[Row] = {

    val window = Window.partitionBy(columnToPartitionBy.head, columnToPartitionBy.tail: _*).orderBy(columnToOrderBy.head,columnToOrderBy.tail: _*)

    filterByRetry(dS, dtClosing).
      withColumn("rn", row_number() over window ).where(col("rn") === 1).drop("rn")//.withColumn(dtValidEnd, to_date(lit("9999-12-31")))

  }


  def filterByRetry(dS: Dataset[Row], dtClosing: String): Dataset[Row] = {

    dS.filter(
      col("dt_valid_start") <= to_date(lit(dtClosing)) &&
        (col("dt_valid_end") > to_date(lit(dtClosing))))
  }

  def filterByRetry(dtClosing: String)(dS: Dataset[Row]): Dataset[Row] = {

    dS.filter(
      col("dt_valid_start") === to_date(lit(dtClosing))
        && (col("dt_valid_end") > to_date(lit(dtClosing))))
  }

  def filterByDtValidEnd(dS: Dataset[Row], dtClosing: String): Dataset[Row] = {
    dS.filter(col("dt_valid_end") > to_date(lit(dtClosing)))
  }

  /**
   *
   * @param refDs
   * @param dtClosing
   * @param dt_valid_start
   * @param dt_valid_end
   * @return Dataset[Row]
   */
  def filterDate(refDs: Dataset[Row], dtClosing: String, dt_valid_start: String, dt_valid_end: String): Dataset[Row] = {

    refDs.filter(
      col(dt_valid_start) <= to_date(lit(dtClosing)) && (
        col(dt_valid_end) > to_date(lit(dtClosing)) || col(dt_valid_end).isNull)
    )
  }

  /**
   * Filtering t_default on DT_Closing with dt_default_end and dt_default_start, and dt_valid_start and dt_valid_end
   *
   * @param refDs
   * @param dtClosing
   * @return
   */
  def filterDateDefault(refDs: Dataset[Row], dtClosing: String): Dataset[Row] = {
    filterDate(refDs, dtClosing, "dt_record_start", "dt_record_end")
      .filter(
        col("dt_default_start") <= to_date(lit(dtClosing)) && (
          col("dt_default_end") > to_date(lit(dtClosing)) || col("dt_default_end").isNull)
      )
  }

  /**
   *
   * @param dsTable
   * @param dt_valid_end
   * @param session
   * @return Dataset[Row]
   */
  def getTableByDateValidEnd(dsTable: Dataset[Row], dt_valid_end: String)(implicit session: SparkSession): Dataset[Row] = {

    import session.sqlContext.implicits._
    dsTable.where($"dt_valid_end" === dt_valid_end)

  }

  /**
   * Get Joined DataSet with contract, client and rel_contract_client
   *
   * @param mapTables
   * @param columnList
   * @param dt_valid_end
   * @param session
   * @return
   */
  def getLoanCase(mapTables: Map[String, Dataset[Row]], idCtrList: Seq[String], columnList: ColumnList, dt_valid_end: String)(implicit session: SparkSession): Dataset[Row] = {

    import session.sqlContext.implicits._
    println(" LoanCase parquet : ")

    // t_concept_relation filtered
    val joinedKeyCtr = columnList.getColumnsByTableName("key-join-ctr-rel") ++
      columnList.getColumnsByTableName("key-join-rel-clt") ++
      columnList.getColumnsByTableName("table-relation")

    val dsRelation: Dataset[Row] = getTableByDateValidEnd(mapTables("t_concept_rel_ctr_clt"), dt_valid_end)
      .select(joinedKeyCtr.map(col => mapTables("t_concept_rel_ctr_clt")(col)): _*)

    // relation by rename columns
    val renamedDsRelation = renamingColumns(dsRelation, columnList.getRenamedColumnByTableName("table-relation"))

    // t_concept_contract filtered
    val dsContract: Dataset[Row] = idCtrList.isEmpty match {
      case true => getTableByDateValidEnd(mapTables("t_concept_contract"), dt_valid_end)
      case _ => getTableByDateValidEnd(mapTables("t_concept_contract"), dt_valid_end).where($"id_ctr".isin(idCtrList: _*))
    }

    //contract by rename columns
    val renamedDsContract = renamingColumns(dsContract, columnList.getRenamedColumnByTableName("table-contract"))

    // t_concept_client filtered
    val joinedKeyCtl = columnList.getColumnsByTableName("table-client") ++ columnList.getColumnsByTableName("key-join-rel-clt")

    val dsClient: Dataset[Row] = mapTables("t_concept_client") //getTableByDateValidEnd(mapTables("t_concept_client"), dt_valid_end)
      .select(joinedKeyCtl.map(col => mapTables("t_concept_client")(col)): _*)

    // client by rename columns
    val renamedDsClient = renamingColumns(dsClient, columnList.getRenamedColumnByTableName("table-client"))

    renamedDsContract
      .join(renamedDsRelation, columnList.getColumnsByTableName("key-join-ctr-rel"), "inner") //Join contract table and  relation table
      .join(renamedDsClient, columnList.getColumnsByTableName("key-join-rel-clt"), "inner")
      .where($"dt_valid_start_clt" < $"dt_valid_end" and $"dt_valid_start" < $"dt_valid_end_clt") //join result with client table

  }

  /**
   * Check equal from both StructType
   *
   * @param structTypeLeft
   * @param structTypeRight
   * @return
   */
  def isEqual(structTypeLeft: StructType, structTypeRight: StructType): Boolean = {
    structTypeLeft.fields.length == structTypeRight.fields.length && structTypeLeft.forall(field => structTypeRight.fieldNames.contains(field.name))
  }

  def commpareStruct(structHive: StructType, structParquet: StructType) = {
    val fieldsHive=  structHive.fieldNames.toSet
    val fieldsParquet=structParquet.fieldNames.toSet

    if (!(fieldsParquet.diff(fieldsHive).isEmpty) )
      println(s"fields only in parquet : ${fieldsParquet.diff(fieldsHive)}")
    if (!(fieldsHive.diff(fieldsParquet).isEmpty) )
      println(s"fields only in have : ${fieldsHive.diff(fieldsParquet)}")

    var finaleStruct =  new  StructType()

    val fiedlsCommon=fieldsParquet.intersect(fieldsHive)
    for (field <- fiedlsCommon) {
      val fieldHive = structHive(field)
      val fieldParquet = structParquet(field)
      if ((fieldHive.dataType.sql.compareTo(fieldParquet.dataType.sql) != 0) && (fieldHive.dataType.typeName.compareTo("varchar") != 0) && (fieldParquet.dataType.typeName.compareTo("string") == 0) ) {
        println(s"$field -> Hive:${fieldHive.dataType.sql}, Parquet:${fieldParquet.dataType.sql}")
      }

      if (fieldParquet.dataType.typeName.compareTo("string") == 0)
        finaleStruct = finaleStruct.add(fieldHive)
      else
        finaleStruct = finaleStruct.add(fieldParquet)
    }

    finaleStruct
  }

  /**
   * Get StructType from SheetSchema
   *
   * @param sheetSchema
   * @return
   */
  def toStructType(sheetSchema: SheetSchema): StructType = {

    val structList = for (field <- sheetSchema.fields) yield {
      val metaBuilder = new MetadataBuilder()
      field.metadata.map {
        meta => metaBuilder.putString(meta._1, meta._2)
      }
      StructField(field.name, SparkSQL.getDataType(field.typeData), field.nullable, metaBuilder.build())
    }

    StructType.apply(structList)
  }


  /**
   * Function to create the file and the directories needed given a filename as a parameter
   *
   * @param fileName
   * @throws java.io.IOException
   * @return
   */
  @throws(classOf[IOException])
  def getStreamOut(fileName: String): FSDataOutputStream = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    fs.create(new Path(fileName))
  }

  /**
   *
   * @param fullBasefileName
   * @param spark
   * @return
   */
  def createCSVFromDir(fullBasefileName:String, spark:SparkSession)={

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val parentPath = new Path(s"${fullBasefileName}")
    val parentPathToBeDelete = new Path(s"${fullBasefileName}PROV")
    fs.rename(parentPath,parentPathToBeDelete)

    fs.listStatus(parentPathToBeDelete).
      foreach( x=>
        if (! (x.getPath.getName.contains("_SUCCESS")) ) {
          val oldfullName=x.getPath
          val newFullName=new Path(s"${fullBasefileName}")
          println(s"${oldfullName.toString} , ${newFullName.toString}")
          fs.rename(oldfullName,newFullName)
        }
      )
    fs.delete(parentPathToBeDelete,true)
  }


  /**
   * method to save the dataset of String with the path (filename) in which to save the data as a txtfile in the mouchard format (fixed length text file)
   */
  def saveToMouchardFormat(fileName: String, ds: Dataset[String]): Unit = {
    ds
      .coalesce(10)
      .write
      .mode(SaveMode.Overwrite)
      .format("text")
      .save(fileName)
    //ajouter le patternfile dans le cadre du calculation engine a sauvegarder dans le meme directory que le mouchard compressé en.z (zippé)
  }

  def saveToMouchardFormat(fileName: String, ds: Dataset[String], nbParts: Int): Unit = {
    ds
      .coalesce(nbParts)
      .write
      .mode(SaveMode.Overwrite)
      .format("text")
      .save(fileName)
    //ajouter le patternfile dans le cadre du calculation engine a sauvegarder dans le meme directory que le mouchard compressé en.z (zippé)
  }


  /**
   * method to save the dataframe into a parquet file that will be partitionned physically in HDFS to improve the writing performances
   */
  def saveToParquet(filename: String, df: Dataset[Row]): Unit = {
    df
      .write
      .mode(SaveMode.Overwrite)
      .parquet(filename)
  }

  /* *
   * method to load a DataFrame from a Parquet file given its path
   */
  def getDataframeFromParquet(targetPath: String)(implicit session: SparkSession): Dataset[Row] = {

    session
      .read
      .parquet(targetPath)
  }

  /**
   * function to split/convert a row into an Array of String
   */
  def rowSplitter(row: Row): Array[String] = {
    val colNames = row.schema.fieldNames

    /** Convert a DF row to an Array of String  */
    for (colName <- colNames) yield {
      scala.util.Try {
        val value = row.getAs[Any](colName)
        value match {
          case null | "null" => ""
          case _ => value.toString
        }
      }.get
    }
  }
  /*
    /**
      * Get DataSet of Table (input or ouput)
      *
      * @param nameTable
      * @param master
      * @param configCluster
      * @param session
      * @return
      */

    def getHiveTableByName(nameTable: String, master: String)(implicit configCluster: ConfigCluster, session: SparkSession): Try[Dataset[Row]] = {
      println(s"get table $nameTable by load parquet ")

      val inputTable = Try(configCluster.getInputTableByName(nameTable).get)
      val outputTable = Try(configCluster.getOutputTableByName(nameTable).get)

      val table = (inputTable.isSuccess, outputTable.isSuccess) match {
        case (true, _) => (inputTable.toOption, true)
        case (false, true) => (outputTable.toOption, true)
        case (_, _) => (None, false)
      }

      Try(session.read.
        option("mergeSchema", "true").
        parquet(s"${configCluster.rootPath}/${table._1.get.pathParquet}/${table._1.get.nameTable}"))
    }
  */

  /**
   *
   * @param nameTable
   * @param session
   * @param configCluster
   * @param argument
   * @return
   */
  def getHiveTableByName(nameTable: String)(implicit session: SparkSession, configCluster: ConfigCluster, argument: JobArgumentGlobal): Try[Dataset[Row]] = {
    println(s"get table by load parquet $nameTable")

    val inputTable = Try(configCluster.getInputTableByName(nameTable).get)
    val outputTable = Try(configCluster.getOutputTableByName(nameTable).get)

    val table = (inputTable.isSuccess, outputTable.isSuccess) match {
      case (true, _) => (inputTable.toOption, true)
      case (false, true) => (outputTable.toOption, true)
      case (_, _) => (None, false)
    }

    val tablePath = s"${configCluster.rootPath}/${table._1.get.pathParquet}/${table._1.get.nameTable}"
    val isExistsPath = Hdfs.isExistsDirectory(tablePath)

    (argument.master, table._2, isExistsPath) match {
      case ("yarn", true, true) => Try(session.read.option("mergeSchema", "true").parquet(tablePath))
      case (_, true, true) => Try(session.read.option("mergeSchema", "true").parquet(tablePath))
      case (_, _, _) => {
        logger.info(s"Table ${nameTable} is not found in input-data or output-data from config_${argument.master}.json")
        println(s"Table ${nameTable} is not found in input-data or output-data from config_${argument.master}.json")
        //session.stop()
        Try(session.emptyDataFrame)
      }
    }
  }

  /**
   * Get Dataset with ordered columnList
   *
   * @param ds
   * @param myColumnsList
   * @param columnsList
   * @return
   */
  def getOutputColumnList(ds: Dataset[Row], myColumnsList: Seq[String])(implicit columnsList: ColumnList): Dataset[Row] = {
    ds.select(
      columnsList.getOrderRenamedColumn(myColumnsList).map { col => ds(col) }: _*)
  }

  /**
   * Get Values of one Column (you can't use this function to get lot of ROW) !!!
   *
   * @param ds
   * @param colName
   * @param excludedValues
   * @param session
   * @return
   */
  def getColumnValues(ds: Dataset[Row], colName: String, excludedValues: Seq[String])(implicit session: SparkSession): Option[Seq[String]] = {
    import session.implicits._
    Some(
      {
        if (excludedValues.nonEmpty)
          ds.select(colName).distinct().where(!ds.col(colName).isin(excludedValues: _*))
        else
          ds.select(colName).distinct()
      }.sort(desc(colName)).map {
        row => row.getAs[Any](colName).toString
      }.collect().toSeq)
  }

  /**
   * method to join two DataFrames based on keys with different names in the two DFs
   *
   * @param leftDF
   * @param rightDF
   * @param left_on
   * @param right_on
   * @param how
   * @return
   */
  def merge(leftDF: DataFrame, rightDF: DataFrame, left_on: Seq[String], right_on: Seq[String], how: String): DataFrame = {
    val joinExpr = left_on.zip(right_on).foldLeft(lit(true)) {
      case (acc, (leftKey, rightKey)) => acc and (leftDF(leftKey) === rightDF(rightKey))
    }
    leftDF.join(rightDF, joinExpr, how)
  }

  def getJsonSchema[T: ClassTag](pathBase: String, nameFile: String, mode: String)(implicit session: SparkSession): T = {
    JsonMapper.toMyClass[T] {
      Hdfs.fromFile(s"$pathBase/$nameFile.json")
    }
  }

  /**
   *
   * @param nameFile
   * @param nameSheet
   * @param verXls
   * @param sparkSession
   * @param configCluster
   * @return SheetSchema
   */
  def getSESchemaFromFile(pathBase: String, nameFile: String, verXls: String, nameSheet: String, mode: String)(implicit sparkSession: SparkSession, configCluster: ConfigCluster): SheetSchema = {

    val jsonSchema = getJsonSchema[ReferentialConfig](pathBase, nameFile, mode)

    val mapSeSchema = jsonSchema.referential(nameFile)
    val xlsLocation = s"${configCluster.rootPath}/${configCluster.outputRefConfig}/${nameFile}_${verXls}.xls"

    /** Get DataSet from Referential Excel */
    val schemaDS = Util.SparkSQL.trimColumns(Excel.readExcel(xlsLocation, mapSeSchema(nameSheet)))

    schemaDS.schema.fieldNames.foldLeft(schemaDS) {
      (ds, nameCol) => ds.withColumn(nameCol, trim(col(nameCol)))
    }
    schemaDS.show(false)

    /** Create ScheetSchema from Dataset[Row] of XLS */
    SheetSchema("struct", {
      for (row <- schemaDS.rdd.mapPartitionsWithIndex {
        (id, iter) => if (id == 0) iter.drop(1) else iter
      }) yield {

        Field(row.getString(0).trim().toLowerCase,
          getDTypeBySchemaSE(row.getString(2).trim(), row.getDouble(3)),
          false,
          Map("comment" -> Try(row.getString(1).trim()).getOrElse("Not defined")))
      }
    }.collect().toList)
  }

  /**
   *
   * @param format
   * @param longueur
   * @return String
   */
  def getDTypeBySchemaSE(format: String, longueur: Double): String = {
    (format.toLowerCase, longueur.toInt) match {
      case ("c", x) => s"varchar($x)"
      case ("n", x) if x <= 10 => "int"
      case ("n", x) if x > 10 => "long"
      case _ => "string"
    }
  }

}
*/
