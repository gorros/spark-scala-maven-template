package com.github.gorros

import org.apache.spark.sql.SparkSession

trait SSession {
    val sparkSession: SparkSession
}

object SSession {
    def createSparkSession(appName: String): SparkSession = {
        val builder = SparkSession
            .builder()
            .appName(appName)

        if (java.lang.Boolean.getBoolean("debug")) {
            builder.master("local[*]")
        }

        builder.getOrCreate()
    }
}
