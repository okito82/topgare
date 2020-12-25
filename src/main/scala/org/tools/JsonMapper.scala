package org.tools

/*
import java.io._
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{DataType, StructType}
import scala.reflect._


/**
 * allows mapping between different case classes and json files
 */
object JsonMapper {
  protected val logger = Logger.getLogger(this.getClass.getName)

  val MyMapper = new ObjectMapper() with ScalaObjectMapper
  MyMapper.registerModule(DefaultScalaModule)
  MyMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  /**
   * Convert json content to StructType
   * @param json string type
   * @return StructType
   */
  def deserializeSchema(json: String): StructType = {
    DataType.fromJson(json).asInstanceOf[StructType]
  }

  /**
   * Convert map {key, value} to string
   * @param value
   * @return
   */
  def toJson(value: Map[Symbol, Any]): String = {
    toJson(value map { case (k, v) => k.name -> v })
  }

  def toJson(value: Any): String = {
    MyMapper.writeValueAsString(value)
  }

  def toMap[T: ClassTag](json: String) = toMyClass[Map[String, T]](json)

  /**
   * Convert string content of json file to my case class
   * @param jsonContent string type
   * @tparam T
   * @throws java.io.IOException
   * @return my case class
   */
  @throws(classOf[IOException])
  def toMyClass[T: ClassTag](jsonContent: String): T = {
    MyMapper.readValue[T](jsonContent, classTag[T].runtimeClass.asInstanceOf[Class[T]])
  }

  /**
   * deprecated in next version
   * A deleted : toClusterConfig, toHiveTable, toColumnsList, toReferentialConfig, toSchema, toJobParameters
   * changed by toMyClass
   */
  @deprecated("replaced toMyClass","1.2")
  @throws(classOf[IOException])
  def toClusterConfig(fileName: String): ConfigCluster = {
    toMyClass[ConfigCluster](BasicFunction.fromResources(fileName).get)
  }

  @deprecated("replaced toMyClass","1.2")
  @throws(classOf[IOException])
  def toColumnsList(fileName: String): ColumnList = {
    toMyClass[ColumnList](BasicFunction.fromResources(fileName).get)
  }

  @deprecated("replaced toMyClass","1.2")
  @throws(classOf[IOException])
  def toSchema(fileName: String): SheetSchema = {
    toMyClass[SheetSchema](BasicFunction.fromResources(fileName).get)
  }
/*
  @deprecated("replaced toMyClass","1.2")
  @throws(classOf[IOException])
  def toJobParameters(fileName: String): JobParameters = {
    toMyClass[JobParameters](BasicFunction.fromResources(fileName).get)
  }
 */
  /*
  @deprecated("replaced toMyClass","1.2")
  @throws(classOf[IOException])
  def toHiveTable(fileName: String): HiveTable = {
    toMyClass[HiveTable](BasicFunction.fromResources(fileName).get)
  }
 */
  /*
  @deprecated("replaced toMyClass","1.2")
  @throws(classOf[IOException])
  def toReferentialConfig(fileName: String): ReferentialConfig = {
    toMyClass[ReferentialConfig](BasicFunction.fromResources(fileName).get)
  }
  */
}

 */