package org

import org.junit._
import Assert._
import org.UDF.{somme, udfTest}
import org.apache.spark.sql.Encoders
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
    import sparkSession.implicits._

    //val df = list.toDS//.toDF("BorneA","BorneB")
      //.withColumn("ckck",udfTest('Borne,'Date))
      //case class Student(BorneA: String, BorneB: String)
    //val studentEncoder = Encoders.product[Student]
   // studentEncoder.schema.printTreeString()
     //val tt = df.as[Student].collectAsList.toString
   // println(tt)
    case class Rec(id: String, value: Int)

    val df = Seq(
      ("first", 1,0),
      ("test", 2,0),
      ("choose", 3,9)
    ).toDF("id","value","uu")

   //val tt = df.rdd.map(r =>(r(0),r(1)+r(2))).collectAsMap()
   //println(tt)

    //df.show()

  assertTrue (somme("A","A") == "gagner")
 //assertTrue (somme("A","A") == "perdu")
}

//    @Test
//    def testKO() = assertTrue(false)

}


