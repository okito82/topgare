package org

import org.junit._
import Assert._
import org.UDF.{somme, udfTest}
import org.scalatest.funsuite.AnyFunSuite


class AppTest  {
  val sparkSession = ApplicationContext.getSparkSession("TopGare")
  import sparkSession.implicits._
  val list = Seq(
    ("A","B"),
    ("B","B"),
    ("B","A"),
    ("A","A"),
    ("A","B"),
    ("B","B")
  )

  @Test
  def testOK() {

    val df = list.toDF("Borne","Date")
      .withColumn("ckck",udfTest('Borne,'Date))
    df.show()

  assertTrue (somme("A","A") == "gagner")
 //assertTrue (somme("A","A") == "perdu")
}

//    @Test
//    def testKO() = assertTrue(false)

}


