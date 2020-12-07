package org

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import scala.util.Try
import java.io._
import scala.reflect.{ClassTag, classTag}


object TestJackson  {


  def fromResource(fileName: String): Try[String] = {
    val fileURL = getClass.getClassLoader.getResource(fileName)
    val file = fileURL.openStream()
    Try {
      scala.io.Source.fromInputStream(file).getLines.mkString("\n")
    }
  }

  val  list_column = scala.io.Source.fromFile("src/main/resources/01/list_table.json").mkString

  val mapper = new ObjectMapper

  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)


  @throws(classOf[IOException])
  def toMyclass[T: ClassTag](jsonContent: String): T = {
    mapper.readValue[T](jsonContent, classTag[T].runtimeClass.asInstanceOf[Class[T]])
  }


}
