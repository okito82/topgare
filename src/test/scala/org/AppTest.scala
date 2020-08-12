package org

import org.junit._
import Assert._

import org.scalatest.funsuite.AnyFunSuite


class AppTest {
  val sparkSession = ApplicationContext.getSparkSession("TopGare")
  import sparkSession.implicits._

  @Test
  def testOK() {

    val list = Seq(
      ("A","2019-07-28","okit"),
      ("A","2019-07-28","okit"),
      ("A","2019-07-28","okit"),
      ("A","2019-07-28","okit"),
      ("A","2019-07-28","okit"),
      ("A","2019-07-28","okit")
    )
    val df = list.toDF("Borne","Date","personne")
     // .withColumn("")
    df.show()

  assertTrue (true)
}

//    @Test
//    def testKO() = assertTrue(false)

}


