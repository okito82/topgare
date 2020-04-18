package org

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.ObjectMapper

import scala.util.Try


object TestJackson extends App {


  def fromResource(fileName: String): Try[String] = {
    val fileURL = getClass.getClassLoader.getResource(fileName)
    val file = fileURL.openStream()
    Try {
      scala.io.Source.fromInputStream(file).getLines.mkString("\n")
    }
  }
  //scala.io.Source.fromInputStream()

  val  list_column = scala.io.Source.fromFile("src/main/resources/DataJson").mkString//fromResource("src/main/resources/Datajson.json").mk
  println(list_column)

  val data = """
    {"some": "json data"}
"""

  val mapper = new ObjectMapper

  mapper.registerModule(DefaultScalaModule)
 val tes = mapper.readValue(list_column, classOf[Map[String, String]])

  println(tes)
}
