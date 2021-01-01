package org
/*
package CTLM.fr.bnpp.pf.rio.calculEngine.newModels.histo


import CTLM.fr.bnpp.pf.rio.calculEngine.Utils.Udf
import CTLM.fr.bnpp.pf.rio.calculEngine.newModels.CommonModels._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql._
import CTLM.fr.bnpp.pf.rio.calculEngine.Utils.Udf._
import CTLM.fr.bnpp.pf.rio.calculEngine.newModels.CommonModels
import CTLM.fr.bnpp.pf.rio.calculEngine.newModels.histo.HistoTable.getLastDayMonth
import org.tools.{ColumnList, ConfigCluster}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object HistoAggregate {
  private val logger = Logger.getLogger(this.getClass.getName)

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  /**
   * FUNCTION TO BUILD TOP BEFORE JOIN WITH TABLE DEFAULT BOX
   * TOP_CARTE, TOP_AMORT, TOP_ACTIVITE
   * @param dsInput
   * @param session
   * @param configClusterCE
   * @return
   */
  def buildTopPreSegmentation(dsInput: Dataset[Row])(implicit session: SparkSession, configClusterCE: ConfigCluster): Dataset[Row] = {
    import session.implicits._

    val winSpecCtr = Window.partitionBy("ID_CONTRACT", "ID_FACILITY", "CD_ROLE").orderBy('DT_CLOSING.desc).rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    // READ FILE TOP_PRESEGMENTATION.JSON
    val jsonDs = CommonModels.jsonToDataFrame("top_carte_amort_plan")
    // GET  CD_PROD_COM LIST OF TOP_CARTE From JSON
    val listTopCarte = jsonDs.select("LIST").where('TOP_PRESEG === "TOP_CARTE").
      withColumn("topCarteValues", explode('LIST)).select("topCarteValues").map(_.getString(0))collect()
    val udfIsTopCarte = udf((cd_prod_type_ctr: String, cd_prod_com_ctr: String) => if (cd_prod_type_ctr != "CL" && !listTopCarte.exists(_ equals cd_prod_com_ctr)) 1 else 0)
    // GET  CD_PROD_COM LIST OF TOP_AMORT From JSON
    val listTopAmort = jsonDs.select("LIST").where('TOP_PRESEG === "TOP_AMORT").
      withColumn("topAmortValues", explode('LIST)).select("topAmortValues").map(_.getString(0))collect()
    val udfIsTopAmort = udf((cd_prod_type_ctr: String, cd_prod_com_ctr: String) => if (cd_prod_type_ctr == "CL" || listTopAmort.exists(_ equals cd_prod_com_ctr)) 1 else 0)

    //READ FILE TOP_AMORT_AUTO_PROD_COM
    val jsonTopAmortAutoProdComDs = CommonModels.jsonToDataFrame("top_amort_auto_prod_com")
    /*.withColumn("CD_PROD_COM_LIST", explode(col("CD_PROD_COM_LIST")))
    .withColumn("PRODNV", concat_ws(",",col("PRODNV1"), col("PRODNV2"), col("PRODNV3")))
    .withColumn("map_PROD", map(col("CD_PROD_COM_LIST"), col("PRODNV")))
    .drop("CD_PROD_COM_LIST", "PRODNV", "PRODNV1", "PRODNV2", "PRODNV3")*/
    val mapProdCom = jsonTopAmortAutoProdComDs.
      withColumn("key",explode('CD_PROD_COM_LIST)). //transforme le tableau en ligne
      select("key","PRODNV1","PRODNV2","PRODNV3").
      map(r=>r.getString(0)->Seq(r.getString(1),r.getString(2),r.getString(3)) ).
      collect().toMap
    val udfProdNV = udf((cd_prod_com_ctr: String) =>  getPRODNV(mapProdCom,cd_prod_com_ctr))

    //READ FILE TOP_AMORT_AUTO_PROD_SOCFIN
    val jsonTopAmortAutoSocfinDs = CommonModels.jsonToDataFrame("top_amort_auto_socfin")
      .withColumn("ID_SOCFIN_LIST", explode(col("ID_SOCFIN_LIST")))
      .withColumn("LIBEL", concat_ws(",",col("LIBELSOC"), col("LIBELRGPT")))
      .withColumn("SOCFIN_CD_ACC_PROD", concat_ws(",",col("ID_SOCFIN_LIST"), col("CD_ACCOUNTING_PRODNUM") ))
      .withColumn("map_socfin", map('ID_SOCFIN_LIST, 'LIBEL ))
      .withColumn("map_socfin_accProd", map('SOCFIN_CD_ACC_PROD, 'LIBEL ))

    val listsocfin = jsonTopAmortAutoSocfinDs.select("map_socfin").collect.map(r=> r.getMap(0).asInstanceOf[Map[String, String]]).toSeq
    val listaccProd = jsonTopAmortAutoSocfinDs.select("map_socfin_accProd").collect.map(r=> r.getMap(0).asInstanceOf[Map[String, String]]).toSeq
    val udfGetLibelSoc = udf((id_socfin: String, id_socfin_cd_acc: String) =>  getLibelSoc(listaccProd,id_socfin_cd_acc,listsocfin, id_socfin))

    val dsOutput = dsInput
      .withColumn("TOP_CARTE", udfIsTopCarte('CD_PROD_TYPE_CTR, 'CD_PROD_COM_CTR))
      .withColumn("TOP_AMORT", udfIsTopAmort('CD_PROD_TYPE_CTR, 'CD_PROD_COM_CTR))
      .withColumn("TOP_ACTIVITE", udfTopActivite('ENCOURS_CTR))
      .withColumn("PRODNVs", udfProdNV('CD_PROD_COM_CTR))
      .withColumn("PRODNV1", 'PRODNVs(0))
      .withColumn("PRODNV2", 'PRODNVs(1))
      .withColumn("PRODNV3",'PRODNVs(2))
      .drop("PRODNVs")
      .withColumn("ID_SOCFIN_AC_PROD",concat_ws(",",'ID_SOCFIN,'CD_ACCOUNTING_PRODNUM))
      .withColumn("LIBELSOC",udfGetLibelSoc('ID_SOCFIN,'ID_SOCFIN_AC_PROD)(0))
      .withColumn("LIBELRGPT",udfGetLibelSoc('ID_SOCFIN,'ID_SOCFIN_AC_PROD)(1))
      .withColumn("TOP_AMORT_AUTO", udfTopAmortAuto('LIBELRGPT,'PRODNV1))
      .withColumn("TOP_HB_ELEVE", when('AMT_OFF_BALANCE_CTR >= 30000, 1).otherwise(0))
      .withColumn("TOP_HB_FAIBLE", when('AMT_OFF_BALANCE_CTR < 30000 && 'AMT_OFF_BALANCE_CTR > 0 , 1).otherwise(0))
      .withColumn("TOP_HB_POSITIF", when('AMT_OFF_BALANCE_CTR > 0 , 1).otherwise(0))
      .withColumn("TOP_HB_NUL", when('AMT_OFF_BALANCE_CTR <= 0  , 1).otherwise(0))
    //dsOutput.show(false)
    dsOutput
  }

  /**
   * FUNCTION TO EXECUTE AFTER JOIN WITH TABLE DEFAULT_BOX
   * TOP_DEFAUT_INSTANTANE, TOP_PLAN
   * @param dsInput
   * @param session
   * @param configClusterCE
   * @return
   */
  def buildTopPreSegDefault(dsInput: Dataset[Row], lastDayDtClosing:String)(implicit session: SparkSession, configClusterCE: ConfigCluster): Dataset[Row] = {

    import session.implicits._
    val jsonDs = CommonModels.jsonToDataFrame("top_carte_amort_plan")
    val winCTR = Window.partitionBy("ID_CONTRACT").orderBy('DT_DEFAULT_START.asc).rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    val winSpectrEvt = Window.partitionBy("ID_CONTRACT").orderBy('DT_DEFAULT_START.asc)
    // GET DEFAULT CRETERION LIST OF TOP_PLAN
    val listTopPlan = jsonDs.select("LIST").where('TOP_PRESEG === "TOP_PLAN").
      withColumn("topPlanValues", explode('LIST)).select("topPlanValues").map(_.getString(0))collect()
    val udfIsTopPlan = udf( (default: String) => if ( listTopPlan.exists(_ equals default) )  1 else 0)
    val ds_res = dsInput
      //.withColumn("IsTopPlan", max(udfIsTopPlan('DEFAULT_CRITERION)) over winCTR)
      .withColumn("DT_CLOSING",lit(lastDayDtClosing))
      .withColumn("isTopDefInstantane",udfTopDefaultInstantanne('DT_DEFAULT_END, 'DT_CLOSING))
      .withColumn("IsTopPlan", udfIsTopPlan('DEFAULT_CRITERION))
      //Calcul du TOP_DEFAUT niveau dossier
      .withColumn("TOP_DEF", udfTopDefaut('DT_DEFAULT_START, 'DT_DEFAULT_END, lit(lastDayDtClosing)))
      //Calcul du TOP_DEFAUT de M-1 à M-12 niveau dossier
      .withColumn("LIST_DEF", udfNbDef('DT_DEFAULT_START, 'DT_DEFAULT_END, lit(lastDayDtClosing)))
      .withColumn("all_top", collect_list('IsTopPlan).over(winSpectrEvt))
      .withColumn("row_number", row_number().over(winSpectrEvt))
      .withColumn("index",udfFindIndex('all_top))
      .withColumn("Top_Plan_EVT", udfTopEvt('row_number,'index))
      .withColumn("myCount",when('DEFAULT_CRITERION isin ("RET30UTP","REPR") and 'Top_Plan_EVT ===1 , 1) otherwise(0))
      .orderBy(asc("DT_DEFAULT_START"))
      //ds_res.select("id_contract","default_criterion","IsTopPlan").orderBy("id_contract").show(300,false)
      .groupBy("ID_CONTRACT")
      .agg(
        max('isTopDefInstantane).alias("TOP_DEF_INSTANTANE"),
        max( 'IsTopPlan ).alias("TOP_PLAN"),
        collect_list("DEFAULT_CRITERION").alias("ALL_DEFAULT_CRITERION"),
        collect_list("DT_DEFAULT_START").alias("ALL_DT_DEFAULT_START"),
        last("DT_DEFAULT_END").alias("DT_DEFAULT_END"),
        sum('myCount).alias("NB_EVT_S_RE_INC"),
        collect_list('LIST_DEF).alias("LIST_DEF_SUM"),
        //Calcul du TOP_DEFAUT niveau contrat
        max('TOP_DEF).alias("TOP_DEFAUT")
      )
      .withColumn("NB_DEF_L12M_CTR", udfNbDefL12MCtr('LIST_DEF_SUM))
    ds_res
  }


  /**
   * Build Columns with Historic
   * @param dsHisto
   * @param session
   * @param configClusterCE
   * @param columnList
   * @return
   */

  def buildAllCollectOnContract(dsHisto: Dataset[Row], dtClosingMo: String)(implicit session: SparkSession, configClusterCE: ConfigCluster,columnList: ColumnList) = {
    println("dsHisto already filtered on id_contract=id_facility. We have one line by contract")
    dsHisto.printSchema()
    import session.implicits._

    val winSpecCtr = Window.partitionBy("ID_CONTRACT").orderBy('DT_CLOSING.desc).rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    val dsreplaced = dsHisto
      .withColumn("DT_LAST_PAYMENT_CTR_NULL", when('DT_LAST_PAYMENT_CTR.isNull, to_date(lit("0001-01-01"))).otherwise('DT_LAST_PAYMENT_CTR))

    /**
     * BUILD COLLECT LIST OF COLUMNS WITH HISTORY
     */
    val dsCollect = dsreplaced
      //.repartition('ID_CONTRACT)
      .withColumn("ALL_DT_LAST_PAYMENT_CTR", collect_list(when('CD_ROLE === "HOLDER", 'DT_LAST_PAYMENT_CTR_NULL).otherwise(null)) over winSpecCtr)
      .withColumn("ALL_DT_LAST_PAYMENT_CTR_OBJECT", collect_list(when('CD_ROLE === "HOLDER", map('DT_CLOSING,'DT_LAST_PAYMENT_CTR_NULL)).otherwise(null)) over winSpecCtr)
      .withColumn("ALL_CD_PROD_TYPE_CTR", collect_list(when('CD_ROLE === "HOLDER", 'CD_PROD_TYPE_CTR).otherwise(null)) over winSpecCtr)
      .withColumn("ALL_CD_PROD_TYPE_CTR_OBJECT", collect_list(when('CD_ROLE === "HOLDER", map('DT_CLOSING,'CD_PROD_TYPE_CTR)).otherwise(null)) over winSpecCtr)
      .withColumn("ALL_ENCOURS_CTR", collect_list(when('CD_ROLE === "HOLDER", 'ENCOURS_CTR.cast(DataTypes.createDecimalType(38, 18))).otherwise(null)) over winSpecCtr)
      .withColumn("ALL_ENCOURS_CTR_OBJECT", collect_list( when('CD_ROLE==="HOLDER", map('DT_CLOSING,'ENCOURS_CTR.cast(DataTypes.createDecimalType(38, 18)))).otherwise(null) ) over winSpecCtr)
      .withColumn("ALL_AMT_DMA_CTR", collect_list(when('CD_ROLE === "HOLDER", 'AMT_DMA_CTR.cast(DataTypes.createDecimalType(38, 18))).otherwise(null)) over winSpecCtr)
      .withColumn("ALL_AMT_DMA_CTR_OBJECT", collect_list(when('CD_ROLE === "HOLDER", map('DT_CLOSING,'AMT_DMA_CTR.cast(DataTypes.createDecimalType(38, 18)))).otherwise(null)) over winSpecCtr)
      .withColumn("ALL_NB_TOT_IMP_CTR", collect_list(when('CD_ROLE === "HOLDER", 'NB_TOT_IMP_CTR.cast(DataTypes.createDecimalType(38, 18))).otherwise(null)) over winSpecCtr)
      .withColumn("ALL_NB_TOT_IMP_CTR_OBJECT", collect_list(when('CD_ROLE === "HOLDER", map('DT_CLOSING, 'NB_TOT_IMP_CTR.cast(DataTypes.createDecimalType(38, 18)))).otherwise(null)) over winSpecCtr)
      .withColumn("ALL_AMT_MONTH_PAY_CTR", collect_list(when('CD_ROLE === "HOLDER", 'AMT_MONTH_PAY_CTR.cast(DataTypes.createDecimalType(38, 18))).otherwise(null)) over winSpecCtr)
      .withColumn("ALL_AMT_MONTH_PAY_CTR_OBJECT", collect_list(when('CD_ROLE === "HOLDER", map('DT_CLOSING, 'AMT_MONTH_PAY_CTR.cast(DataTypes.createDecimalType(38,18)))).otherwise(null)) over winSpecCtr)
      .withColumn("ALL_AMT_PAST_DUE_CTR", collect_list(when('CD_ROLE === "HOLDER", 'AMT_PAST_DUE_CTR.cast(DataTypes.createDecimalType(38, 18))).otherwise(null)) over winSpecCtr)
      .withColumn("ALL_AMT_PAST_DUE_CTR_OBJECT", collect_list(when('CD_ROLE === "HOLDER", map('DT_CLOSING, 'AMT_PAST_DUE_CTR.cast(DataTypes.createDecimalType(38, 18)))).otherwise(null)) over winSpecCtr)
      .withColumn("ALL_CD_SOURCE_CTR", collect_list(when('CD_ROLE === "HOLDER", 'CD_SOURCE_CTR).otherwise(null)) over winSpecCtr)
      .withColumn("ALL_CD_SOURCE_CTR_OBJECT", collect_list(when('CD_ROLE === "HOLDER", map('DT_CLOSING, 'CD_SOURCE_CTR)).otherwise(null)) over winSpecCtr)
      .withColumn("ALL_DT_CREATION_CTR_TODO", collect_list(when('CD_ROLE === "HOLDER", 'DT_CREATION_CTR_NULL).otherwise(null)) over winSpecCtr)
      .withColumn("ALL_REV_TOT", collect_list(when('CD_ROLE === "HOLDER", 'REV_TOT.cast(DataTypes.createDecimalType(38, 18))).otherwise(null)) over winSpecCtr) //Keep carreful, REV_TO is BigDecimal(38,0)
      .withColumn("ALL_AMT_CURRENT_MATURITY_CTR", collect_list(when('CD_ROLE === "HOLDER", 'AMT_CURRENT_MATURITY_CTR.cast(DataTypes.createDecimalType(38, 18))).otherwise(null)) over winSpecCtr)
      .withColumn("ALL_AMT_CURRENT_MATURITY_CTR_OBJECT", collect_list(when('CD_ROLE === "HOLDER", map('DT_CLOSING, 'AMT_CURRENT_MATURITY_CTR.cast(DataTypes.createDecimalType(38, 18)))).otherwise(null)) over winSpecCtr)
      .withColumn("ALL_UTIL_CTR", collect_list(when('CD_ROLE === "HOLDER", 'UTIL_CTR.cast(DataTypes.createDecimalType(38, 18))).otherwise(null)) over winSpecCtr)
      .withColumn("ALL_UTIL_CTR_OBJECT", collect_list(when('CD_ROLE === "HOLDER", map('DT_CLOSING, 'UTIL_CTR.cast(DataTypes.createDecimalType(38, 18)))).otherwise(null)) over winSpecCtr)
      .withColumn("ALL_AMT_OUTSTANDING_CTR", collect_list(when('CD_ROLE === "HOLDER", 'AMT_OUTSTANDING_CTR.cast(DataTypes.createDecimalType(38, 18))).otherwise(null)) over winSpecCtr)
      .withColumn("ALL_AMT_OUTSTANDING_CTR_OBJECT", collect_list(when('CD_ROLE === "HOLDER", map('DT_CLOSING, 'AMT_OUTSTANDING_CTR.cast(DataTypes.createDecimalType(38, 18)))).otherwise(null)) over winSpecCtr)
      //.withColumn("SUM_PAY_L6M_CTR_FOR_ALL", Udf.udfSumPayL6mCtrObject('ALL_AMT_MONTH_PAY_CTR_OBJECT, 'DT_CREATION_CTR).cast(DataTypes.createDecimalType(38, 0)))
      .withColumn("SUM_PAY_L6M_CTR_FOR_ALL", udfSumPayL6mCtrCompObject('ALL_AMT_MONTH_PAY_CTR_OBJECT, 'DT_CREATION_CTR, lit(dtClosingMo)).cast(DataTypes.createDecimalType(38, 0)))

    dsCollect
  }

  def buildRank(ds:Dataset[Row])(implicit session: SparkSession) = {
    import session.implicits._
    // val winSpecCltDtClosing = Window.partitionBy("ID_CLT_EXPO", "DT_CLOSING").orderBy("DT_CLOSING")
    val winSpecCltDtClosing = Window.partitionBy("ID_CLT_EXPO", "DT_CLOSING").orderBy('DT_CLOSING ,'CD_ROLE.desc)
    val ds_withRank = ds
      .withColumn("rank", row_number() over winSpecCltDtClosing)//will be used for ALL_*CLT

    ds_withRank
  }

  def buildAllCollectOnClient(ds: Dataset[Row])(implicit session: SparkSession, configClusterCE: ConfigCluster,columnList: ColumnList) = {
    import session.implicits._
    val winSpecClt = Window.partitionBy("ID_CLT_EXPO").orderBy('DT_CLOSING.desc).rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    ds.
      filter('ID_CLT_EXPO.isNotNull)
      //.repartition('ID_CLT_EXPO,'DT_CLOSING)
      .withColumn("ALL_ENCOURS_CLT_OBJECT", collect_list( when(col("rank") === 1 && 'CD_ROLE==="HOLDER", map('DT_CLOSING,'ENCOURS_CLT.cast(DataTypes.createDecimalType(38, 18)))) ) over winSpecClt)
      .withColumn("ALL_ENCOURS_CLT", collect_list(when(col("rank") === 1 && 'CD_ROLE==="HOLDER", 'ENCOURS_CLT.cast(DataTypes.createDecimalType(38, 18)))) over winSpecClt)
      .withColumn("ALL_AMT_PAST_DUE_CLT_OBJECT", collect_list(when(col("rank") === 1 && 'CD_ROLE==="HOLDER", map('DT_CLOSING,'AMT_PAST_DUE_CLT.cast(DataTypes.createDecimalType(38, 18)))) ) over winSpecClt)
      .withColumn("ALL_AMT_PAST_DUE_CLT", collect_list(when(col("rank") === 1 && 'CD_ROLE==="HOLDER", 'AMT_PAST_DUE_CLT.cast(DataTypes.createDecimalType(38, 18)))) over winSpecClt)
      .withColumn("ALL_NB_TOT_IMP_CLT_OBJECT", collect_list(when(col("rank") === 1 && 'CD_ROLE==="HOLDER", map('DT_CLOSING,'NB_TOT_IMP_CLT.cast(DataTypes.createDecimalType(38, 18)))) ) over winSpecClt)
      .withColumn("ALL_NB_TOT_IMP_CLT", collect_list(when(col("rank") === 1 && 'CD_ROLE==="HOLDER", 'NB_TOT_IMP_CLT.cast(DataTypes.createDecimalType(38, 18)))) over winSpecClt)
      .withColumn("ALL_AMT_MONTH_PAY_CLT", collect_list(when(col("rank") === 1 && 'CD_ROLE==="HOLDER",'AMT_MONTH_PAY_ClT.cast(DataTypes.createDecimalType(38, 18)))) over winSpecClt)
      .withColumn("ALL_AMT_MONTH_PAY_CLT_OBJECT", collect_list(when(col("rank") === 1 && 'CD_ROLE==="HOLDER",map('DT_CLOSING,'AMT_MONTH_PAY_ClT.cast(DataTypes.createDecimalType(38, 18))))) over winSpecClt)
      .withColumn("ALL_AMT_BALANCE_CLT", collect_list(when(col("rank") === 1 && 'CD_ROLE==="HOLDER",'AMT_BALANCE_CLT.cast(DataTypes.createDecimalType(38, 18)))) over winSpecClt)
      .withColumn("ALL_AMT_BALANCE_CLT_OBJECT", collect_list(when(col("rank") === 1 && 'CD_ROLE==="HOLDER",map('DT_CLOSING,'AMT_BALANCE_CLT.cast(DataTypes.createDecimalType(38, 18))))) over winSpecClt)
      .withColumn("ALL_AMT_CURRENT_MATURITY_CLT", collect_list(when(col("rank") === 1 && 'CD_ROLE==="HOLDER",'AMT_CURRENT_MATURITY_CLT.cast(DataTypes.createDecimalType(38, 18)))) over winSpecClt)
      .withColumn("ALL_AMT_CURRENT_MATURITY_CLT_OBJECT", collect_list(when(col("rank") === 1 && 'CD_ROLE==="HOLDER",map('DT_CLOSING,'AMT_CURRENT_MATURITY_CLT.cast(DataTypes.createDecimalType(38, 18)))) ) over winSpecClt)
      .withColumn("ALL_DT_CREATION_CTR_CLT_AVEC_HISTO", collect_list(when(col("rank") === 1 && 'CD_ROLE==="HOLDER",'DT_CREATION_CTR_NULL)) over winSpecClt)
      .withColumn("SUM_PAY_INTER", collect_list(when('CD_ROLE === "HOLDER", map('ID_CONTRACT, 'SUM_PAY_L6M_CTR_FOR_ALL.cast(DataTypes.createDecimalType(38,18)))).otherwise(null)) over winSpecClt)

  }

  def buildAllCollectForSnapshot(dsHisto: Dataset[Row], lastDayDtClosing: String)(implicit session: SparkSession, configClusterCE: ConfigCluster,columnList: ColumnList) = {
    import session.implicits._
    val ds_withRank = /*persistedDF( "ds_withRank",*/buildRank(dsHisto) /*)*/
    val ds_onContract= /*persistedDF( "buildAllCollectOnContract",*/buildAllCollectOnContract(ds_withRank, lastDayDtClosing) /*)*/
    val ds_onClient = /*persistedDF( "buildAllCollectOnClient",*/buildAllCollectOnClient(ds_onContract)/* )*/
      //Keep only month snapshot with its collect list
      .filter('DT_CLOSING === lastDayDtClosing)

    //persistedDF("ds_onClient",ds_onClient)
    ds_onClient
    /*
        // Save snapshot with collect list
        val location = CommonModels.getLocation("dfAggWithCollect")
        ds_onClient.write.mode(SaveMode.Overwrite).parquet(location)
        location
        */
  }

  def getCreateHistoAgg(dsHisto: Dataset[Row], dtClosingM0: String, dataRelativePath: String)(implicit session: SparkSession, configClusterCE: ConfigCluster,columnList: ColumnList): Dataset[Row] = {

    //Nous sommes toujours niveau contrat
    import session.implicits._
    val lastDayDtClosing = getLastDayMonth(dtClosingM0)

    val dsHisto_WithDT_CREATION_CTR_NULL= dsHisto
      .withColumn("DT_CREATION_CTR_NULL", when('DT_CREATION_CTR.isNull, to_date(lit("0001-01-01"))).otherwise('DT_CREATION_CTR))

    val v_buildAllCollectForSnapshot=buildAllCollectForSnapshot(dsHisto_WithDT_CREATION_CTR_NULL,lastDayDtClosing)
    //from here, we have  only DT_CLOSING===lastDayDtClosing

    //val dsCollect=session.read.parquet(v_buildAllCollectForSnapshot)
    /**
     * CALCULATE AGGREGATES
     */
    val winSpecClt = Window.partitionBy("ID_CLT_EXPO").orderBy('ID_CONTRACT.desc).rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    val dsWithHisto = v_buildAllCollectForSnapshot //dsCollect
      // RDG AMT_IMP_CLT *** PD
      //.withColumn("AMT_IMP_CLT_OBSELETE", udfAmtImpCltObselete('ALL_AMT_PAST_DUE_CLT).cast(DataTypes.createDecimalType(38, 0)))
      .withColumn("AMT_IMP_CLT", udfAmtImpCltObject('DT_CREATION_CLT, 'ALL_AMT_PAST_DUE_CLT_OBJECT).cast(DataTypes.createDecimalType(38, 0)))
      // RDG ANC_DER_PAY_L6M_CTR *** ACTIVE CARDS
      //.withColumn("ANC_DER_PAY_L6M_CTR_OBSELETE", udfAncDerPayL6MCtrObselete('ALL_DT_LAST_PAYMENT_CTR, lit(dtClosingM0)))
      .withColumn("ANC_DER_PAY_L6M_CTR", udfAncDerPayL6MCtrObject('ALL_DT_LAST_PAYMENT_CTR_OBJECT, lit(dtClosingM0),'DT_CREATION_CTR))
      // RDG OVERL_CONS_L12M_CTR *** ACTIVE CARDS
      //.withColumn("OVERL_CONS_L12M_CTR_OBSELETE", udfOverConsL12MCtrObselete('ALL_CD_PROD_TYPE_CTR, 'ALL_ENCOURS_CTR, 'ALL_AMT_DMA_CTR))
      .withColumn("OVERL_CONS_L12M_CTR", udfOverConsL12MCtrObject('ALL_CD_PROD_TYPE_CTR_OBJECT, 'ALL_ENCOURS_CTR_OBJECT, 'ALL_AMT_DMA_CTR_OBJECT,'DT_CREATION_CTR))
      // RDG NB_MOIS_RET_L12M_CTR *** ACTIVE CARDS
      //.withColumn("NB_MOIS_RET_L12M_CTR_OBSELETE", udfNbMoisRetL12MCtr('ALL_NB_TOT_IMP_CTR))
      .withColumn("NB_MOIS_RET_L12M_CTR", udfNbMoisRetL12MCtrObject('ALL_NB_TOT_IMP_CTR_OBJECT, 'DT_CREATION_CTR))
      // RDG NB_AGC_BAL_L9M_CLT *** ACTIVE CARDS
      //.withColumn("NB_AGC_BAL_L9M_CLT_OBSELETE", udfNbAgcBalCltObselete('ALL_ENCOURS_CLT, lit(9)))
      .withColumn("NB_AGC_BAL_L9M_CLT", udfNbAgcBalCltObject('ALL_ENCOURS_CLT_OBJECT, 'DT_CREATION_CLT, lit(9)))

      // RDG NB_AGC_BAL_L6M_CLT *** INACTIVE CARDS
      //.withColumn("NB_AGC_BAL_L6M_CLT_OBSELETE", udfNbAgcBalCltObselete('ALL_ENCOURS_CLT, lit(6)))
      .withColumn("NB_AGC_BAL_L6M_CLT", udfNbAgcBalCltObject('ALL_ENCOURS_CLT_OBJECT, 'DT_CREATION_CLT, lit(6)))

      // RDG MEAN_POS_B_L6M_CLT *** ACTIVE CARDS
      //.withColumn("MEAN_POS_B_L6M_CLT_OBSELETE", udfMeanPosBL6MCltObselete('ALL_ENCOURS_CLT).cast(DataTypes.createDecimalType(38, 0)))
      .withColumn("MEAN_POS_B_L6M_CLT", udfMeanPosBL6MCltObject('ALL_ENCOURS_CLT_OBJECT,'DT_CREATION_CLT).cast(DataTypes.createDecimalType(38, 0)))

      //.withColumn("AMT_MAX_RET_L6M_CLT_OBSELETE", udfAmtMaxRetL6MCltObselete('ALL_AMT_PAST_DUE_CLT).cast(DataTypes.createDecimalType(38, 0)))

      .withColumn("DT_CREATION_CLT_AVEC_HISTO", udfMinDate('ALL_DT_CREATION_CTR_CLT_AVEC_HISTO))
      .withColumn("AMT_MAX_RET_L6M_CLT", udfAmtMaxRetL6MCltObject('ALL_AMT_PAST_DUE_CLT_OBJECT,'DT_CREATION_CLT_AVEC_HISTO).cast(DataTypes.createDecimalType(38, 0)))

      //RDG NB_CP_CLT *** ACTIVE CARDS*** INACTIVE CARDS
      .withColumn("NB_CP_CLT", sum(when(col("cd_role") === "HOLDER",col("TOP_CP_CTR")).otherwise(0)) over winSpecClt)
      //RDG NB_CL_CLT *** ACTIVE CARDS*** INACTIVE CARDS *** LOAN
      .withColumn("NB_CL_CLT", sum(when(col("cd_role") === "HOLDER",col("TOP_CL_CTR")).otherwise(0)) over winSpecClt)
      // RDG ANC_DER_IMP_L12M_CLT *** ACTIVE CARDS *** OVERINDEBTEDNESS
      //.withColumn("ANC_DER_IMP_L12M_CLT_OBSELETE", udfAncDerImpL12MCltObselete('ALL_NB_TOT_IMP_CLT))
      .withColumn("ANC_DER_IMP_L12M_CLT", udfAncDerImpL12MCltObject('ALL_NB_TOT_IMP_CLT_OBJECT,'DT_CREATION_CLT_AVEC_HISTO))

      // RDG NB_ACT_SIG_L6M_CTR *** ACTIVE CARDS
      //.withColumn("NB_ACT_SIG_L6M_CTR_OBSELETE", udfNbActSigLMCtrObselete(col("ALL_ENCOURS_CTR"), lit(6)))
      .withColumn("NB_ACT_SIG_L6M_CTR", udfNbActSigLMCtrObject('ALL_ENCOURS_CTR_OBJECT, 'DT_CREATION_CTR,lit(6)))
      //.withColumn("NB_ACT_SIG_L12M_CTR_OBSELETE", udfNbActSigLMCtrObselete(col("ALL_ENCOURS_CTR"), lit(12)))
      .withColumn("NB_ACT_SIG_L12M_CTR", udfNbActSigLMCtrObject('ALL_ENCOURS_CTR_OBJECT, 'DT_CREATION_CTR, lit(12)))

      //.withColumn("RT_BAL1_MAXL3M_CLT_OBSELETE", udf_rt_bal1_max3m_cltObselete('ALL_ENCOURS_CLT).cast(DataTypes.createDecimalType(38, 0)))
      .withColumn("RT_BAL1_MAXL3M_CLT", udf_rt_bal1_max3m_cltObject('ALL_ENCOURS_CLT_OBJECT,'DT_CREATION_CLT).cast(DataTypes.createDecimalType(38, 0)))

      // RDG NB_ACT_L6M_CLT *** INACTIVE CARDS
      //.withColumn("NB_ACT_L6M_CLT_OBSELETE", udfNbActL6MCltObselete('ALL_ENCOURS_CLT))
      .withColumn("NB_ACT_L6M_CLT", udfNbActL6MCltObject('ALL_ENCOURS_CLT_OBJECT,'DT_CREATION_CLT))

      // RDG NB_MOIS_RET_L12M_CLT *** ACTIVE CARDS
      //.withColumn("NB_MOIS_RET_L12M_CLT_OBSELETE", udfNbMoisRetL12MCltObselete('ALL_NB_TOT_IMP_CLT))
      .withColumn("NB_MOIS_RET_L12M_CLT", udfNbMoisRetL12MCltObject('ALL_NB_TOT_IMP_CLT_OBJECT, 'DT_CREATION_CLT))

      // RDG RT_SREC_BAL_L12M_CLT
      //.withColumn("RT_SREC_BAL_L12M_CLT_OBSELETE", udfRtSrecBalL12mCltObselete(col("ALL_ENCOURS_CLT"), col("ALL_AMT_PAST_DUE_CLT"), col("ALL_CD_SOURCE_CTR")).cast(DataTypes.createDecimalType(38,0)))
      .withColumn("RT_SREC_BAL_L12M_CLT", udfRtSrecBalL12mCltObject('DT_CREATION_CLT_AVEC_HISTO, 'DT_CREATION_CTR,'ALL_ENCOURS_CLT_OBJECT, 'ALL_AMT_PAST_DUE_CLT_OBJECT, 'ALL_CD_SOURCE_CTR_OBJECT).cast(DataTypes.createDecimalType(38,0)))

      // RDG NB_AGC_BAL_L12M_CLT *** AUTOMOTIVE
      //.withColumn("NB_AGC_BAL_L12M_CLT_OBSELETE", udfNbAgcBalCltObselete('ALL_ENCOURS_CLT, lit(12)))
      .withColumn("NB_AGC_BAL_L12M_CLT", udfNbAgcBalCltObject('ALL_ENCOURS_CLT_OBJECT,'DT_CREATION_CLT, lit(12)))

      //RDG RT_BAL1_MAXL6M_CLT *** AUTOMOTIVE
      //.withColumn("RT_BAL1_MAXL6M_CLT_OBSELETE", udf_rt_bal1_maxl6m_cltObselete('ALL_ENCOURS_CLT).cast(DataTypes.createDecimalType(38, 0)))
      .withColumn("RT_BAL1_MAXL6M_CLT", udf_rt_bal1_maxl6m_cltObject('DT_CREATION_CLT, 'ALL_ENCOURS_CLT_OBJECT).cast(DataTypes.createDecimalType(38, 0)))

      //RDG ANC_CTR *** AUTOMOTIVE *** AUTOMOTIVE
      .withColumn("ANC_CTR", udfAncCtr('ALL_DT_CREATION_CTR_TODO, lit(dtClosingM0)))
      //RDG PAY_CONS_L3M_CTR *** AUTOMOTIVE
      //.withColumn("PAY_CONS_L3M_CTR_OBSELETE", udfPayConsL3MCtrObselete('ANC_CTR, 'ALL_AMT_MONTH_PAY_CTR))
      .withColumn("PAY_CONS_L3M_CTR", udfPayConsL3MCtrObject('ANC_CTR, 'DT_CREATION_CTR, 'ALL_AMT_MONTH_PAY_CTR_OBJECT))
      // RDG RT_ENC_REVTOT_CTR *** LOAN
      .withColumn("RT_ENC_REVTOT_CTR", udfRtEncRevtotCtr('ALL_REV_TOT, 'ALL_ENCOURS_CTR).cast(DataTypes.createDecimalType(38, 0)))
      //RDG RT_SREC_REVTOT_CTR *** LOAN
      .withColumn("RT_SREC_REVTOT_CTR", udfrtSrecRevtotCtr('ALL_REV_TOT, 'ALL_AMT_PAST_DUE_CTR).cast(DataTypes.createDecimalType(38, 0)))
      //RDG RT1_SREC_BAL_L12M_CLT *** AUTOMOTIVE
      //.withColumn("RT1_SREC_BAL_L12M_CLT_BIS_OBSELETE", udfRt1SrecBalL12mCltBisObselete('ALL_CD_SOURCE_CTR, 'ALL_AMT_CURRENT_MATURITY_CLT, 'ALL_AMT_PAST_DUE_CLT,  'ALL_ENCOURS_CLT).cast(DataTypes.createDecimalType(38, 0)))
      .withColumn("RT1_SREC_BAL_L12M_CLT_BIS", udfRt1SrecBalL12mCltBisObject('DT_CREATION_CLT_AVEC_HISTO, 'DT_CREATION_CTR,'ALL_CD_SOURCE_CTR_OBJECT, 'ALL_AMT_CURRENT_MATURITY_CLT_OBJECT, 'ALL_AMT_PAST_DUE_CLT_OBJECT,  'ALL_ENCOURS_CLT_OBJECT).cast(DataTypes.createDecimalType(38, 0)))
      .withColumn("RT1_SREC_BAL_L12M_CLT", udfRt1SrecBalL12mClt('RT1_SREC_BAL_L12M_CLT_BIS,  'ANC_CTR).cast(DataTypes.createDecimalType(38, 0)))
      // RDG NB_MOIS_IMP_L9M_CLT *** LOAN
      //.withColumn("NB_MOIS_IMP_L9M_CLT_OBSELETE", udfNbMoisImpLMCltObselete('ALL_NB_TOT_IMP_CLT, lit(9)))
      .withColumn("NB_MOIS_IMP_L9M_CLT", udfNbMoisImpLMCltObject('ALL_NB_TOT_IMP_CLT_OBJECT, 'DT_CREATION_CLT_AVEC_HISTO,lit(9)))

      // RDG NB_MOIS_IMP_L12M_CLT *** LGDS
      //.withColumn("NB_MOIS_IMP_L12M_CLT", udfNbMoisImpLMClt('ALL_NB_TOT_IMP_CLT, lit(12))) TODO effacer apres confirmation
      .withColumn("NB_MOIS_IMP_L12M_CLT", udfNbMoisImpLMCltObject('ALL_NB_TOT_IMP_CLT_OBJECT,'DT_CREATION_CLT, lit(12)))

      // RDG  RT_PAYL6M_MNS7_CTR ***LOAN
      //.withColumn("RT_PAYL6M_MNS7_CTR_OBSELETE", udfRtPayl6mMns7CtrObselete('ALL_CD_SOURCE_CTR, 'ALL_AMT_MONTH_PAY_CTR, 'ANC_CTR, 'ALL_AMT_CURRENT_MATURITY_CTR).cast(DataTypes.createDecimalType(38, 0)))
      .withColumn("RT_PAYL6M_MNS7_CTR", udfRtPayl6mMns7CtrObject('ALL_CD_SOURCE_CTR_OBJECT, 'ALL_AMT_MONTH_PAY_CTR_OBJECT, 'ANC_CTR, 'ALL_AMT_CURRENT_MATURITY_CTR_OBJECT, 'DT_CREATION_CTR).cast(DataTypes.createDecimalType(38, 0)))

      // RDG ANC_DER_PAY_L9M_CTR *** LOAN
      //.withColumn("ANC_DER_PAY_L9M_CTR_OBSELETE", udfAncDerPayL9MCtrObselete('ALL_DT_LAST_PAYMENT_CTR, 'ANC_CTR, lit(dtClosingM0)))
      .withColumn("ANC_DER_PAY_L9M_CTR", udfAncDerPayL9MCtrObject('ALL_DT_LAST_PAYMENT_CTR_OBJECT, 'ANC_CTR, lit(dtClosingM0), 'DT_CREATION_CTR))

      //RDG RT_MIMPL3_IMPL4_CLT *** LOAN
      //.withColumn("RT_MIMPL3_IMPL4_CLT_OBSELETE", udfRtMimpl3Impl4CltObselete('ALL_AMT_PAST_DUE_CLT).cast(DataTypes.createDecimalType(38, 0)))
      .withColumn("RT_MIMPL3_IMPL4_CLT", udfRtMimpl3Impl4CltObject('ALL_AMT_PAST_DUE_CLT_OBJECT).cast(DataTypes.createDecimalType(38, 0)))

      // RDG NB_AG_BAL_L12M_CLT *** LOAN
      //.withColumn("NB_AG_BAL_L12M_CLT_OBSELETE", udfNbAgBalCltObselete('ALL_ENCOURS_CLT, lit(12)))
      .withColumn("NB_AG_BAL_L12M_CLT", udfNbAgBalCltObject('ALL_ENCOURS_CLT_OBJECT, 'DT_CREATION_CLT, lit(12)))

      // RDG RT_PAYL3M_BAL4_CLT *** OVERINDEBTEDNESS
      //.withColumn("RT_PAYL3M_BAL4_CLT_OBSELETE", udfRtPayL3mBaL4CltObselete('ALL_ENCOURS_CLT, 'ALL_AMT_MONTH_PAY_CLT).cast(DataTypes.createDecimalType(38, 0)))
      .withColumn("RT_PAYL3M_BAL4_CLT", udfRtPayL3mBaL4CltObject('DT_CREATION_CLT_AVEC_HISTO,'ALL_ENCOURS_CLT_OBJECT, 'ALL_AMT_MONTH_PAY_CLT_OBJECT).cast(DataTypes.createDecimalType(38, 0)))

      // RDG NB_IMP_CLT *** PD *** EAD *** LGDS
      //.withColumn("NB_IMP_CLT_OBSELETE", udfNbImpCltObselete('ALL_NB_TOT_IMP_CLT).cast(DataTypes.createDecimalType(12, 0)))
      .withColumn("NB_IMP_CLT", udfNbImpCltObject('DT_CREATION_CLT,'ALL_NB_TOT_IMP_CLT_OBJECT).cast(DataTypes.createDecimalType(12, 0)))

      // RDG NB_DMC_BAL_L3M_CTR *** EAD
      //.withColumn("NB_DMC_BAL_L3M_CTR_OBSOLETE", udfNbDmcBalL3MCTR('ALL_ENCOURS_CTR))  // TODO JB delete after validation
      .withColumn("NB_DMC_BAL_L3M_CTR", udfNbDmcBalL3MCtrObject('ALL_ENCOURS_CTR_OBJECT,'DT_CREATION_CTR))

      // RDG NB_DM_BAL_L12M_CLT *** EAD
      //.withColumn("NB_DM_BAL_L12M_CLT_OBSELETE", udfNbDmBalL12MClt('ALL_ENCOURS_CLT))
      .withColumn("NB_DM_BAL_L12M_CLT", udfNbDmBalL12MCltObject('ALL_ENCOURS_CLT_OBJECT,'DT_CREATION_CLT))

      // RDG NB_AGC_BAL_L6M_CTR *** EAD
      //.withColumn("NB_AGC_BAL_L6M_CTR_OBSOLETE", udfNbAgcBalCtr('ALL_ENCOURS_CTR, lit(6))) // TODO JB delete after validation
      .withColumn("NB_AGC_BAL_L6M_CTR", udfNbAgcBalCtrObjet('ALL_ENCOURS_CTR_OBJECT,'DT_CREATION_CTR, lit(6)))

      // RDG NB_MOIS_IMP_L6M_CLT *** EAD
      //.withColumn("NB_MOIS_IMP_L6M_CLT_OBSOLETE", udfNbMoisImpL6MClt('ALL_NB_TOT_IMP_CLT)) // TODO JB delete after validation
      .withColumn("NB_MOIS_IMP_L6M_CLT", udfNbMoisImpL6MCltObject('ALL_NB_TOT_IMP_CLT_OBJECT,'DT_CREATION_CLT))

      // RDG IMP_CONS_L3M_CLT *** EAD
      //.withColumn("IMP_CONS_L3M_CLT_OBSOLETE", udfImpConsLmClt('ALL_NB_TOT_IMP_CLT, lit(3))) // TODO JB delete after validation
      .withColumn("IMP_CONS_L3M_CLT", udfImpConsLmCltObject('ALL_NB_TOT_IMP_CLT_OBJECT,'DT_CREATION_CLT, lit(3)))


      // RDG IMP_CONS_L12M_CLT *** LGDS
      .withColumn("IMP_CONS_L12M_CLT", udfImpConsLmClt('ALL_NB_TOT_IMP_CLT, lit(12)))
      //RDG MAX_UTIL_L12M_CTR *** EAD
      //.withColumn("MAX_UTIL_L12M_CTR_OBSOLETE", udfMaxUtilL12MCtr('ALL_UTIL_CTR).cast(DataTypes.createDecimalType(38, 0))) // TODO JB delete after validation
      .withColumn("MAX_UTIL_L12M_CTR", udfMaxUtilL12MCtrObjet('ALL_UTIL_CTR_OBJECT, 'DT_CREATION_CTR).cast(DataTypes.createDecimalType(38, 0)))

      //RDG NB_IMPCIMP_L12M_CLT *** LGDS
      .withColumn("NB_IMPCIMP_L12M_CLT", udfNbImpcimpClt('NB_MOIS_IMP_L12M_CLT, 'IMP_CONS_L12M_CLT).cast(DataTypes.createDecimalType(38, 0)))

      //RDG NB_AG_BAL_L12M_CTR*** LGDS
      //.withColumn("NB_AG_BAL_L12M_CTR_OBSOLETE", udfNbAgBalCtr('ALL_ENCOURS_CTR, lit(12)))
      .withColumn("NB_AG_BAL_L12M_CTR", udfNbAgBalCtrObject('ALL_ENCOURS_CTR_OBJECT, 'DT_CREATION_CTR, lit(12)))

      // RDG NB_MOIS_RET_L6M_CTR *** LGDS
      //.withColumn("NB_MOIS_RET_L6M_CTR_OBSELETE", udfNbMoisRetL6MCtr('ALL_NB_TOT_IMP_CTR).cast(DataTypes.createDecimalType(38, 0)))
      .withColumn("NB_MOIS_RET_L6M_CTR", udfNbMoisRetL6MCtrObject('ALL_NB_TOT_IMP_CTR_OBJECT, 'DT_CREATION_CTR).cast(DataTypes.createDecimalType(38, 0)))

      // RDG MAX_BAL_L12M_CTR *** LGDS
      //.withColumn("MAX_BAL_L12M_CTR_OBSELETE", udfMaxBalL12mCtr('ALL_ENCOURS_CTR).cast(DataTypes.createDecimalType(38, 0)))
      .withColumn("MAX_BAL_L12M_CTR", udfMaxBalL12mCtrObject('ALL_ENCOURS_CTR_OBJECT, 'DT_CREATION_CTR).cast(DataTypes.createDecimalType(38, 0)))

      // RDG MAX_BAL_L12M_CLT *** LGDS *** LGDD
      //.withColumn("MAX_BAL_L12M_CLT_OBSELETE", udfMaxBalL12mClt('ALL_ENCOURS_CLT).cast(DataTypes.createDecimalType(38, 0))) TODO effacer une fois confirmer
      .withColumn("MAX_BAL_L12M_CLT", udfMaxBalL12mCltObject('ALL_ENCOURS_CLT_OBJECT,'DT_CREATION_CLT,lit(12)).cast(DataTypes.createDecimalType(38, 0)))

      // RDG SUM_PAY_L6M_CTR *** LGDS *** LGDD
      //.withColumn("SUM_PAY_L6M_CTR_OBSELETE", udfSumPayL6mCtr('ALL_AMT_MONTH_PAY_CTR).cast(DataTypes.createDecimalType(38, 0)))
      .withColumn("SUM_PAY_L6M_CTR", udfSumPayL6mCtrObject('ALL_AMT_MONTH_PAY_CTR_OBJECT, 'DT_CREATION_CTR).cast(DataTypes.createDecimalType(38, 0)))
      .withColumn("SUM_PAY_L6M_CTR_MAJ", udfSumPayL6mCtrCompObject('ALL_AMT_MONTH_PAY_CTR_OBJECT, 'DT_CREATION_CTR, lit(dtClosingM0)).cast(DataTypes.createDecimalType(38, 0)))

      // RDG NB_DMC_BAL_L12M_CLT *** LGDS
      //.withColumn("NB_DMC_BAL_L12M_CLT_OSBSELETE", udfNbDmcBalL12MCLT('ALL_ENCOURS_CLT))
      .withColumn("NB_DMC_BAL_L12M_CLT", udfNbDmcBalL12MCLTObjet('ALL_ENCOURS_CLT_OBJECT,'DT_CREATION_CLT))

      // RDG NB_MOIS_IMP_L12M_CLT *** LGDS
      .withColumn("NB_MOIS_IMP_L12M_CLT", udfNbMoisImpL12MClt('ALL_NB_TOT_IMP_CLT))

      //RDG NB_MOIS_PAY_L3M_CLT  *** LGDS
      //.withColumn("NB_MOIS_PAY_L3M_CLT_OBSELETE",udfNbMoisPayL3mClt('ALL_AMT_MONTH_PAY_CLT))
      .withColumn("NB_MOIS_PAY_L3M_CLT",udfNbMoisPayL3mCltObjet('ALL_AMT_MONTH_PAY_CLT_OBJECT, 'DT_CREATION_CLT))

      //RDG SUM_PAY_L3M_CTR  *** LGDS
      //.withColumn("SUM_PAY_L3M_CTR_OBSELETE", udfSumPayL3MCtr('ALL_AMT_MONTH_PAY_CTR).cast(DataTypes.createDecimalType(38, 0))) TODO efffacer une fois confirmer
      .withColumn("SUM_PAY_L3M_CTR", udfSumPayL3MCtrObject('ALL_AMT_MONTH_PAY_CTR_OBJECT,'DT_CREATION_CTR,lit(3)).cast(DataTypes.createDecimalType(38, 0)))

      // RDG RT_BAL1_MAXL12M_CTR *** LGDS *** LGDD
      //.withColumn("RT_BAL1_MAXL12M_CTR_OBSELETE", udf_rt_bal1_maxl12m_ctr('ALL_ENCOURS_CTR,'MAX_BAL_L12M_CTR).cast(DataTypes.createDecimalType(38, 0)))
      .withColumn("RT_BAL1_MAXL12M_CTR", udf_rt_bal1_maxl12m_ctr_object('ALL_ENCOURS_CTR_OBJECT, 'DT_CREATION_CTR, 'MAX_BAL_L12M_CTR).cast(DataTypes.createDecimalType(38, 0)))

      // RDG RT_BAL1_MAXL12M_CLT *** LGDS *** LGDD
      //.withColumn("RT_BAL1_MAXL12M_CLT_OBSELETE", udf_rt_bal1_maxl12m_clt('ALL_ENCOURS_CLT,'MAX_BAL_L12M_CLT).cast(DataTypes.createDecimalType(38, 0))) TODO effacer apres confirmation
      .withColumn("RT_BAL1_MAXL12M_CLT", udf_rt_bal1_maxl12m_cltObject('ALL_ENCOURS_CLT_OBJECT,'MAX_BAL_L12M_CLT,'DT_CREATION_CLT,lit(12)).cast(DataTypes.createDecimalType(38, 0)))
      // RDG RT_PAYL6M_BAL0_CTR *** LGDS
      //.withColumn("RT_PAYL6M_BAL0_CTR_OBSELETE",udfRtPayl6mBal0Ctr('ALL_ENCOURS_CTR,'SUM_PAY_L6M_CTR).cast(DataTypes.createDecimalType(38, 0)))
      .withColumn("RT_PAYL6M_BAL0_CTR",udfRtPayl6mBal0CtrObject('ALL_ENCOURS_CTR_OBJECT, 'DT_CREATION_CTR, 'SUM_PAY_L6M_CTR).cast(DataTypes.createDecimalType(38, 0)))

      // RDG NB_TOT_IMP_L12M_CLT  *** LGDS *** LGDD
      //.withColumn("NB_TOT_IMP_L12M_CLT_OBSELETE", udfNbTotImpL12mClt('ALL_NB_TOT_IMP_CLT).cast(DataTypes.createDecimalType(38, 0))) // TODO effacer apres confirmation
      .withColumn("NB_TOT_IMP_L12M_CLT", udfNbTotImpL12mCltObject('ALL_NB_TOT_IMP_CLT_OBJECT,'DT_CREATION_CLT).cast(DataTypes.createDecimalType(38, 0)))


      // RDG IMP_CONS_L12M_CLT *** LGDS
      //.withColumn("IMP_CONS_L12M_CLT", udfImpConsL12mClt('ALL_NB_TOT_IMP_CLT)) TODO effacer apres comfirmation
      .withColumn("IMP_CONS_L12M_CLT", udfImpConsL12mCltObject('ALL_NB_TOT_IMP_CLT_OBJECT,'DT_CREATION_CLT,lit(12)))

      // RDG NB_DMC_BAL_L3M_CLT *** LGDS
      //.withColumn("NB_DMC_BAL_L3M_CLT_OBSELETE", udfNbDmcBalL3MClt('ALL_ENCOURS_CLT))
      .withColumn("NB_DMC_BAL_L3M_CLT", udfNbDmcBalL3MCltObject('ALL_ENCOURS_CLT_OBJECT,'DT_CREATION_CLT,lit(3)))
      // RDG SUM_PAY_L3M_CLT  *** LGDS
      .withColumn("SUM_PAY_L3M_CLT", sum(when(col("cd_role") === "HOLDER", col("SUM_PAY_L3M_CTR")).otherwise(0)) over winSpecClt)
      // RDG RT_PAYL3M_BAL0_CLT  *** LGDS
      //.withColumn("RT_PAYL3M_BAL0_CLT_OBSELETE", udfRtPayL3mBalanceClt('ALL_AMT_BALANCE_CLT,'SUM_PAY_L3M_CLT).cast(DataTypes.createDecimalType(30, 0))) TODO effacer apres confirmation
      .withColumn("RT_PAYL3M_BAL0_CLT", udfRtPayL3mBalanceCltObject('ALL_AMT_BALANCE_CLT_OBJECT,'SUM_PAY_L3M_CLT,'DT_CREATION_CLT,lit(3)).cast(DataTypes.createDecimalType(30, 0)))
      //RDG EVO_UTIL_L3M_CTR *** LGDS
      //.withColumn("EVO_UTIL_L3M_CTR_OBSELETE", udfEvoUtilL3mCtr('ALL_CD_PROD_TYPE_CTR, 'ALL_UTIL_CTR, 'ALL_AMT_DMA_CTR, 'ALL_AMT_OUTSTANDING_CTR, 'ALL_AMT_PAST_DUE_CTR).cast(DataTypes.createDecimalType(30, 0)))
      .withColumn("EVO_UTIL_L3M_CTR", udfEvoUtilL3mCtrObject('ALL_CD_PROD_TYPE_CTR_OBJECT, 'ALL_UTIL_CTR_OBJECT, 'ALL_AMT_DMA_CTR_OBJECT, 'ALL_AMT_OUTSTANDING_CTR_OBJECT, 'ALL_AMT_PAST_DUE_CTR_OBJECT, 'DT_CREATION_CTR).cast(DataTypes.createDecimalType(30, 0)))
      //RDG SUM_PAY_L6M_CLT *** LGDD

      .withColumn("SUM_PAY_L6M_CLT", udfSumPay('SUM_PAY_INTER))
      //On récupère les SUM_PAY_L6M_CTR associé à chaque contrat autant de fois que ces contrats apparaissent. On vient mapper chaque contrat à son sum_pay_l6m_ctr autant de fois qu'il y a de lignes
      //.withColumn("SUM_PAY_INTER", collect_list(when('CD_ROLE === "HOLDER", map('ID_CONTRACT, 'SUM_PAY_L6M_CTR.cast(DataTypes.createDecimalType(38,18)))).otherwise(null)) over winSpecClt)
      //On applique un flatten pour s'assurer n'avoir pour chaque contrat qu'une seule fois son sum_pay_l6m_ctr puis que cette valeur est unique par contrat
      //.withColumn("SUM_PAY_TEST", udfSumPay('SUM_PAY_INTER))
      //RDG RT_PAYL6M_BAL0_CLT *** LGDD
      .withColumn("RT_PAYL6M_BAL0_CLT", udfPayL6mBalClt('AMT_BALANCE_CLT, 'SUM_PAY_L6M_CLT).cast(DataTypes.createDecimalType(30, 0)))
      //RDG SUM_BAL_L3M_CLT *** LGDD
      //.withColumn("SUM_BAL_L3M_CLT_OBSELETE", udfSumBalClt('ALL_ENCOURS_CLT, lit(0), lit(3)).cast(DataTypes.createDecimalType(38, 0))) TODO EFFACER apres confirmation
      .withColumn("SUM_BAL_L3M_CLT", udfSumBalCltObject('ALL_ENCOURS_CLT_OBJECT,'DT_CREATION_CLT,lit(3), lit(0), lit(3)).cast(DataTypes.createDecimalType(38, 0)))
      //RDG SUM_PAY_L12M_CTR  *** LGDD
      //.withColumn("SUM_PAY_L12M_CTR_OBSELETE", udfSumPayLMCtr('ALL_AMT_MONTH_PAY_CTR, lit(12)).cast(DataTypes.createDecimalType(38, 0)))
      .withColumn("SUM_PAY_L12M_CTR", udfSumPayLMCtrObject('ALL_AMT_MONTH_PAY_CTR_OBJECT, 'DT_CREATION_CTR, lit(12)).cast(DataTypes.createDecimalType(38, 0)))
      //RT_BAL1_MAXL3M_CTR *** LGDD
      //.withColumn("RT_BAL1_MAXL3M_CTR_OBSOLETE", udfRtBal1MaxL3mCtrObsolete('ALL_ENCOURS_CTR).cast(DataTypes.createDecimalType(30, 0)))
      .withColumn("RT_BAL1_MAXL3M_CTR", udfRtBal1MaxL3mCtrObject('ALL_ENCOURS_CTR_OBJECT , 'DT_CREATION_CTR).cast(DataTypes.createDecimalType(30, 0)))

      //RT_PAYL12M_BAL0_CTR *** LGDD
      //.withColumn("RT_PAYL12M_BAL0_CTR_OBSELETE", udfRtPayL12mBAL0CTR('ALL_ENCOURS_CTR, 'SUM_PAY_L12M_CTR).cast(DataTypes.createDecimalType(30, 0)))
      .withColumn("RT_PAYL12M_BAL0_CTR", udfRtPayL12mBAL0CTRObject('ALL_ENCOURS_CTR_OBJECT, 'DT_CREATION_CTR, 'SUM_PAY_L12M_CTR).cast(DataTypes.createDecimalType(30, 0)))
      //UT_DIM_CS_L3M_CTR *** LGDD
      //.withColumn("UT_DIM_CS_L3M_CTR_OBSOLETE", udfUtDimCsL3mCtrObsolete('ALL_UTIL_CTR).cast(DataTypes.createDecimalType(12, 0)))
      .withColumn("UT_DIM_CS_L3M_CTR", udfUtDimCsL3mCtrObject('ALL_UTIL_CTR_OBJECT , 'DT_CREATION_CTR).cast(DataTypes.createDecimalType(12, 0)))
      //RDG SUM_BAL_L46M_CLT *** LGDD
      //.withColumn("SUM_BAL_L46M_CLT_OBSELETE", udfSumBalClt('ALL_ENCOURS_CLT, lit(3), lit(6)).cast(DataTypes.createDecimalType(38, 0))) TODO EFFAcer apres confrimation
      .withColumn("SUM_BAL_L46M_CLT", udfSumBalCltObject('ALL_ENCOURS_CLT_OBJECT,'DT_CREATION_CLT,lit(6), lit(3), lit(6)).cast(DataTypes.createDecimalType(38, 0)))
      //RDG RT_BAL_L3ML6M_CLT *** LGDD
      .withColumn("RT_BAL_L3ML6M_CLT", udfRtBalL3mL6mClt('SUM_BAL_L3M_CLT, 'SUM_BAL_L46M_CLT).cast(DataTypes.createDecimalType(30, 0)))
      //RDG NB_IMP_CTR  *** LGDD
      //.withColumn("NB_IMP_CTR_OBSELETE", udfNbImpCtr('ALL_NB_TOT_IMP_CTR).cast(DataTypes.createDecimalType(38, 0))) //TODO effacer apres confirmation
      .withColumn("NB_IMP_CTR", udfNbImpCtrObject('ALL_NB_TOT_IMP_CTR_OBJECT,'DT_CREATION_CTR,lit(12)).cast(DataTypes.createDecimalType(38, 0)))

      //RDG RET_CONS_L3M_CTR *** LGDD
      //.withColumn("RET_CONS_L3M_CTR_OBSOLETE", udfRetConsCtrObsolete('ALL_NB_TOT_IMP_CTR , lit(3)).cast(DataTypes.createDecimalType(12, 0)))
      .withColumn("RET_CONS_L3M_CTR", udfRetConsCtrObject('ALL_NB_TOT_IMP_CTR_OBJECT ,  lit(3)).cast(DataTypes.createDecimalType(12, 0)))

      //RDG NB_AGC_BAL_L3M_CTR *** LGDD . Maj Mehdi
      //.withColumn("NB_AGC_BAL_L3M_CTR_OBSOLETE", udfNbAgcBalCtr('ALL_ENCOURS_CTR, lit(3)).cast(DataTypes.createDecimalType(12, 0)))
      .withColumn("NB_AGC_BAL_L3M_CTR", udfNbAgcBalCtrObjet('ALL_ENCOURS_CTR_OBJECT, 'DT_CREATION_CTR, lit(3)).cast(DataTypes.createDecimalType(12, 0)))

      //RDG NB_MAX_RET_L3M_CTR *** LGDD
      ////////////////////.withColumn("NB_MAX_RET_L3M_CTR_OBSOLETE", udfMaxRetCtrObsolete('ALL_NB_TOT_IMP_CTR , lit(3)).cast(DataTypes.createDecimalType(12, 0)))
      .withColumn("NB_MAX_RET_L3M_CTR", udfMaxRetCtrObject('ALL_NB_TOT_IMP_CTR_OBJECT , 'DT_CREATION_CTR, lit(3)).cast(DataTypes.createDecimalType(12, 0)))
      //RDG SUM_PAY_L9M_CTR *** LGDD
      //////////////////.withColumn("SUM_PAY_L9M_CTR_OBSOLETE", udfSumL9MCtrObsolete('ALL_AMT_MONTH_PAY_CTR))
      .withColumn("SUM_PAY_L9M_CTR", udfSumL9MCtrObject('ALL_AMT_MONTH_PAY_CTR_OBJECT,'DT_CREATION_CTR).cast(DataTypes.createDecimalType(38, 0)))
      //RDG SUM_BAL_L9M_CTR *** LGDD
      /////////////////.withColumn("SUM_BAL_L9M_CTR_OBSOLETE", udfSumL9MCtrObsolete('ALL_ENCOURS_CTR))
      .withColumn("SUM_BAL_L9M_CTR", udfSumL9MCtrObject('ALL_ENCOURS_CTR_OBJECT,'DT_CREATION_CTR).cast(DataTypes.createDecimalType(38, 0)))
      //RDG RT_PAYL8M_BAL9_CTR *** LGDD
      .withColumn("RT_PAYL8M_BAL9_CTR", udfRtPayL8MBaL9Ctr('SUM_PAY_L9M_CTR,'SUM_BAL_L9M_CTR).cast(DataTypes.createDecimalType(30, 0)))
      //RDG NB_MOIS_PAY_L3_CTR *** LGDD
      ///////////////////.withColumn("NB_MOIS_PAY_L3M_CTR_OBSOLETE", udfNbMoisPayLCtrObsolete('ALL_AMT_MONTH_PAY_CTR, 'DT_CREATION_CTR ,lit(3)))
      .withColumn("NB_MOIS_PAY_L3M_CTR", udfNbMoisPayLCtrObject('ALL_AMT_MONTH_PAY_CTR_OBJECT, 'DT_CREATION_CTR ,lit(3)))
      //RDG NB_MOIS_PAY_L6M_CTR *** LGDD
      ///////////////////.withColumn("NB_MOIS_PAY_L6M_CTR_OBSOLETE", udfNbMoisPayLCtrObsolete('ALL_AMT_MONTH_PAY_CTR,'DT_CREATION_CTR, lit(6)))
      .withColumn("NB_MOIS_PAY_L6M_CTR", udfNbMoisPayLCtrObject('ALL_AMT_MONTH_PAY_CTR_OBJECT,'DT_CREATION_CTR, lit(6)))
      //RDG RT_PAY_L6M_CTR *** LGDD
      .withColumn("RT_PAY_L6M_CTR",  udfRtPayL6MCtr('NB_MOIS_PAY_L6M_CTR).cast(DataTypes.createDecimalType(30, 0)))
      //RDG NB_TOT_IMP_L6M_CTR  *** LGDD
      /////////////////////.withColumn("NB_TOT_IMP_L6M_CTR_OBSOLETE", udfNbTotImpL6MCtrObsolete('ALL_NB_TOT_IMP_CTR).cast(DataTypes.createDecimalType(12, 0)))
      .withColumn("NB_TOT_IMP_L6M_CTR", udfNbTotImpL6MCtrObject('ALL_NB_TOT_IMP_CTR_OBJECT, 'DT_CREATION_CTR).cast(DataTypes.createDecimalType(12, 0)))
      //RDG ANC_DER_IMP_L3M_CTR  *** LGDD
      ///////////////////.withColumn("ANC_DER_IMP_L3M_CTR_OBSOLETE", udfAncDerImpL3MCtrObsolete('ALL_NB_TOT_IMP_CTR).cast(DataTypes.createDecimalType(12, 0)))
      .withColumn("ANC_DER_IMP_L3M_CTR", udfAncDerImpL3MCtrObject('ALL_NB_TOT_IMP_CTR_OBJECT,'DT_CREATION_CTR).cast(DataTypes.createDecimalType(12, 0)))

    // dsWithHisto.show(false)
    // Build TOP PRESEGMANTATION TOP_CARTE, TOP_ACTIVITE, TOP_AMORT
    val dsTopPreSegmentation = buildTopPreSegmentation(dsWithHisto)
      .drop("ALL_DT_LAST_PAYMENT_CTR","ALL_CD_PROD_TYPE_CTR","ALL_ENCOURS_CTR", "ALL_ENCOURS_CTR","ALL_AMT_DMA_CTR",
        "ALL_NB_TOT_IMP_CLT", "ALL_NB_TOT_IMP_CTR", "ALL_AMT_MONTH_PAY_CTR",
        "ALL_AMT_PAST_DUE_CLT", "ALL_AMT_PAST_DUE_CTR", "ALL_CD_SOURCE_CTR", "ALL_REV_TOT",
        "ALL_AMT_CURRENT_MATURITY_CTR","ALL_AMT_MONTH_PAY_CLT","ALL_AMT_BALANCE_CLT","ALL_UTIL_CTR","DT_LAST_PAYMENT_CTR_NULL",
        "DT_CREATION_CTR_NULL","ALL_AMT_CURRENT_MATURITY_CLT", "ALL_AMT_OUTSTANDING_CTR", "SUM_PAY_INTER", "SUM_PAY_L6M_CTR_FOR_ALL")

    if (session.sparkContext.master.startsWith("local")) {
      println(s"before default count : ${dsTopPreSegmentation.count()}")
    }

    // Join DS HISTORIC WITH TABLE DEFAULT_BOX
    val defaultRenamed= CommonModels.loadDefault(dataRelativePath, dtClosingM0)

    // Build TOP PRESEGMANTATION OF DEFAULT : TOP_PLAN, TOP_DEFAUT_INSTANTANE
    val dsTopPreSegDefault = buildTopPreSegDefault(defaultRenamed, lastDayDtClosing)
      .drop("ID_CTR").drop("DT_CLOSING")
      .drop("AMT_BALANCE") //Is the same ?
      .drop("ID_CTR_PRINC").drop("ID_CLT_HOLDER").drop("ID_CLT_COHOLDER").drop("ID_CLT_LOCAL_HOLDER")
      .drop("ID_CLT_LOCAL_COHOLDER").drop("CD_SOURCE").drop("LIST_DEF_SUM")

    val dsResult = CommonModels.joinDefault(dsTopPreSegmentation,dsTopPreSegDefault)
      //RDG PRESEGMENT PD
      .withColumn("PRESEGMENT_PD", udfPresegmentPD('TOP_CARTE,'TOP_ACTIVITE,'TOP_DEF_INSTANTANE,'TOP_PLAN,'TOP_AMORT,'TOP_AMORT_AUTO))
      .withColumn("PRESEGMENT_CF", udfPresegmentCF('TOP_CARTE,'TOP_DEF_INSTANTANE,'TOP_PLAN,'TOP_AMORT,'TOP_HB_ELEVE,'TOP_HB_FAIBLE,'TOP_HB_POSITIF,'TOP_HB_NUL))
      //RDG ID_MODELE_LGD / ID_MODELE_LGD_SCORE / ID_MODELE_LGD_ARBRE *** LGDS ***
      .withColumn("ID_MODELE", udfIdModelLGD('TOP_DEF_INSTANTANE, 'TOP_PLAN, 'TOP_CARTE, 'TOP_AMORT, 'TOP_ACTIVITE))
      .withColumn("ID_MODELE_LGD_SCORE", $"ID_MODELE._1")
      .withColumn("ID_MODELE_LGD_ARBRE", $"ID_MODELE._2")
      .withColumn("ID_MODELE_LGD", $"ID_MODELE._3")
      .withColumn("PRESEGMENT_LGDD", udfPresegmentLGDD('TOP_CARTE, 'TOP_ACTIVITE,'top_amort, 'top_amort_auto))
      .withColumn("TOP_DEFAUT", when('TOP_DEFAUT.isNull, 0).otherwise('TOP_DEFAUT))
      .withColumn("NB_DEF_L12M_CTR", when('NB_DEF_L12M_CTR.isNull, 0).otherwise('NB_DEF_L12M_CTR))
      .drop("ID_MODELE")

    //Build DEFAULT SEQUENCE
    val dsSequencePath = CommonModels.buildSnapshotSeqDef(dtClosingM0)

    CommonModels.cacheCE()

    val dsSequence = session.read.parquet(dsSequencePath)
    val dsResultWithSeq = dsResult.join(dsSequence, Seq("ID_CONTRACT"), "LEFT")
      .withColumn("PERIMETRE_LGDD", udfPerimetreLGDD('TOP_DEF_INSTANTANE, 'CD_REF_MOMENT, 'TOP_NON_CURE))

    // LOAD Histo_event
    val dsEventHisto= CommonModels.loadEventHisto(dataRelativePath,dtClosingM0)
    // Collect all columns of Histo and calculate MVT_FIN_TOT_CLT and MAX_MT_FIN_L12M_CLT *** LGDS
    val listColumnsEvt= columnList.getColumnsByTableName("rdg_event")
    val dsEvtRdg = CommonModels.collectEventHisto(dsEventHisto,dtClosingM0 )
      .select(listColumnsEvt.map(c => col(c)): _*)
      .distinct()
    val winSpecCltEvt = Window.partitionBy("ID_CLT_EXPO").orderBy('DT_CLOSING.desc).rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    val dsResultEvt= dsResultWithSeq.join(dsEvtRdg, Seq("ID_CONTRACT","ID_CLT_EXPO","CD_ROLE"),"LEFT")
    //.withColumn("ALL_MVT_OUTSTANDING_OBJECT",collect_list(when('CD_ROLE === "HOLDER", map('DT_CLOSING,'MVT_OUTSTANDING.cast(DataTypes.createDecimalType(38, 18)))).otherwise(null)) over winSpecCltEvt)
    //.withColumn("MVT_OUTSTANDING_CLT",udfSommeObject('ALL_MVT_OUTSTANDING_OBJECT,'DT_CREATION_CLT,lit(12)))
    //.withColumn("ALL_MVT_CASH_FINANCING_OBJECT",collect_list(when('CD_ROLE === "HOLDER", map('DT_CLOSING,'MVT_CASH_FINANCING.cast(DataTypes.createDecimalType(38, 18)))).otherwise(null)) over winSpecCltEvt)
    //.withColumn("MVT_CASH_FINANCING_CLT",udfSommeObject('ALL_MVT_OUTSTANDING_OBJECT,'DT_CREATION_CLT,lit(12)))
    //.withColumn("MVT_FIN_TOT_CLT", 'MVT_OUTSTANDING_CLT + 'MVT_CASH_FINANCING_CLT)
    //.withColumn("ALL_MVT_FIN_TOT_CLT_OBJECT", collect_list(when('rank === 1 && 'CD_ROLE ==="HOLDER",map('DT_CLOSING,'MVT_FIN_TOT_CLT.cast(DataTypes.createDecimalType(38,18)))).otherwise(null)) over winSpecCltEvt)
    //.withColumn("MAX_MT_FIN_L12M_CLT", udfMaxMtFinL12mCltObject('ALL_MVT_FIN_TOT_CLT_OBJECT,'DT_CREATION_CLT,lit(12)) )
    //.drop("ALL_MVT_CASH_FINANCING_OBJECT")
    dsResultEvt
  }
}
 */