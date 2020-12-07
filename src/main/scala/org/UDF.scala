package org


import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StringType

object UDF {



  def somme(pileA : String, pileB: String): String = {
    (pileA,pileB) match {
      case("A","A") => "gagner"
      case("A","B") => "perdu"
      case("B","A") => "perdu"
      case("B","B") => "gagner"
      case("C",_) => "gagner"
    }

  }
  val udfTest : UserDefinedFunction  = udf[String,String,String](somme)

}
