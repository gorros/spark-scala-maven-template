package com.github.gorros

object Main {
    def main(args: Array[String]): Unit = {
        System.setProperty("debug","true")
        val sc = SSession.createSparkSession("Main")
        import sc.implicits._
        val userDf = Seq(
            (1, "Gor"),
            (2, "Mike"),
            (3, "Jack")
        ).toDF("id", "name")
        userDf.show()
        userDf.printSchema()
        println(userDf.count())
    }
}
