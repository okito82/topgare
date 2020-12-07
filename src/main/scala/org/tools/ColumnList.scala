package org.tools


import com.fasterxml.jackson.annotation.JsonProperty

import scala.collection.mutable.LinkedHashMap

case class ColumnList(@JsonProperty("columns-list") getMapColumns : Map[String, LinkedHashMap[String,String] ]){

  def getColumnsByTableName(tableName: String): List[String] = {
    getMapColumns(tableName).keys.toList
  }

  def getOrderRenamedColumn(myList: Seq[String]): List [String] = {
    val columnList = myList.flatMap{
      table =>
        renamingColumns(table)
    }.toList
    println(s" Order Renamed Columns List : \n\t ${columnList.mkString(" -")}")
      columnList

  }

  def getOrderColumns(myList : Seq[String]): List[String] = {
    val columnList = myList.flatMap(table => getColumnsByTableName(table)).toList
    println(s" Columns List : \n\t ${columnList.mkString(" -")}")
      columnList
  }

  def renamingColumns(tableName: String): Seq[String] = {
    val columnsMap = getMapColumns(tableName)

    if (isRenamedColumns(tableName)){
      {
        for (col <- columnsMap) yield  {
          if( col._1 == col._2 || col._2.isEmpty) col._1 else col._2
        }
      }.toSeq
    }
    else
      columnsMap.keys.toSeq
  }

  def getRenamedColumnsByTableName(tableName: String): LinkedHashMap[String, String] = {
    getMapColumns(tableName).filter(pair => pair._2.nonEmpty && !pair._1.equals(pair._2))
  }

  def isRenamedColumns(tableName: String): Boolean = {
    getRenamedColumnsByTableName(tableName).nonEmpty
  }


}
