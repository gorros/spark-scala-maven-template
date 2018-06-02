package com.github.gorros
import junit.framework.TestCase
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.Assert.assertEquals
import org.junit.Test

class SteamingTest extends TestCase {
    @Test
    def testSpark(): Unit = {
        System.setProperty("debug","true")
        val spark = SSession.createSparkSession("test")
        import spark.implicits._
        val userDf = Seq(
            (1, "Gor"),
            (2, "Mike"),
            (3, "Jack")
        ).toDF("id", "name")
        userDf.show()

        val testDir = "/tmp/test"
        userDf.write.mode(SaveMode.Overwrite).json(testDir)

        // Streaming
        val jsonDf = spark.readStream.schema(userDf.schema).json(testDir)
        val query = jsonDf.writeStream.trigger(Trigger.Once()).format("memory").queryName("test").start()
        query.awaitTermination()
        spark.sql("select * from test").show()

        val df = spark.sql("select * from test")
        assertEquals(df.count(), userDf.count())
    }
}
