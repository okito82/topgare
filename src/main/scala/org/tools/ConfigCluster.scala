package org.tools
/*
import com.fasterxml.jackson.annotation.JsonProperty


/**
 * it is important for the recovery of the various configurations useful for the running of our projects: Perimeter, CalculEngine and Reporting IOB/OFS/BMRC,
 *
 * @param rootPath nameNode Path witn here port
 * @param outputRefCsv where i store csv file Referential
 * @param outputRefConfig where i get excel Referential
 * @param inputRefSchema recover schemas referential
 * @param getInputTables allow to get input tables
 * @param getOutputTables allox to get output tables
 */
case class ConfigCluster(@JsonProperty("root-path") var rootPath: String,
                         @JsonProperty("output-ref-csv") outputRefCsv: String,
                         @JsonProperty("output-ref-config") outputRefConfig: String,
                         @JsonProperty("input-schema-ref") inputRefSchema: String,
                         @JsonProperty("input-data") getInputTables: Map[String, HiveTable],
                         @JsonProperty("output-data") getOutputTables: Map[String, HiveTable]) {

  // Get input tables from our config
  def getInputTableByName(nameTable: String): Option[HiveTable] = Some(getInputTables(nameTable))

  // Get output tables from our config
  def getOutputTableByName(nameTable: String): Option[HiveTable] = Some(getOutputTables(nameTable))

  // Print this config
  override def toString: String = {
    s"""
       | root-path : $rootPath
       | output-ref-config : $outputRefConfig
       | output-ref-csv : $outputRefCsv
       | input-data :
       | ${getInputTables.map(_._2.toString).mkString("","\n","")}
       | output-data :
       | ${getOutputTables.map(_._2.toString).mkString("","\n","")}
    """.stripMargin
  }

}

 */