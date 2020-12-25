package org.tools


import com.fasterxml.jackson.annotation.JsonProperty
import org.aopalliance.reflect.Field



/**
 * used to serialize and deserialize SheetSchema from/to file
 *
 * @param typeSource
 * @param fields
 */
case class SheetSchema(@JsonProperty("type") typeSource: String = "struct", @JsonProperty("fields") fields: List[Field]) {
  override def toString = {
    s"""{
         "type" : "$typeSource",
         "fields": [
         ${fields.map(_.toString).mkString("",",","")}
         ]
     }
    """.stripMargin
  }

  def Json = toString
}
