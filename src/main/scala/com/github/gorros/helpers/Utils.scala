package com.github.gorros.helpers

import org.apache.spark.sql.{DataFrame, SparkSession}


object Utils {

    def createTableFromDf(df: DataFrame, db: String, table: String, path: String, partitionColumn: String = ""): Unit = {
        val fields = df.schema.fields.map(f => s"${f.name} ${f.dataType.typeName}")
        val createTableSQL =
            if (partitionColumn.isEmpty) {
                s"""
                   |CREATE TABLE $db.$table(${fields.mkString(",")})
                   |USING parquet
                   |OPTIONS (path '$path')
        """.stripMargin
            } else {
                s"""
                   |CREATE TABLE $db.$table(${fields.mkString(",")})
                   |USING parquet
                   |PARTITIONED BY ($partitionColumn)
                   |LOCATION '$path'
        """.stripMargin
            }

        df.sparkSession.sql(s"DROP TABLE IF EXISTS $db.$table")
        df.sparkSession.sql(createTableSQL)
    }

    def updatePartitions(sparkSession: SparkSession, db: String, table: String): Unit = {
        sparkSession.sql(s"MSCK REPAIR TABLE $db.$table")
    }

    def tableExists(sparkSession: SparkSession, db: String, table: String): Boolean = {
        sparkSession.catalog.tableExists(db, table)
    }

    def schemaIsChanged(df: DataFrame, db: String, table: String): Boolean = {
        val currentColumns = df.sparkSession.sql(s"DESCRIBE $db.$table").select("col_name")
            .collect().map(r => r.getString(0)).toSet
        val newColumns = df.schema.fields.map(_.name).toSet
        newColumns != currentColumns
    }
}
