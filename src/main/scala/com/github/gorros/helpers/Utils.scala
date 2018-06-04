package com.github.gorros.helpers

import java.net.URI

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.sys.process._


object Utils {

    def flatten(df: DataFrame): DataFrame = {
        def flattenSchema(schema: StructType, prefix: String = null): Array[Column] = {
            schema.fields.flatMap(f => {
                val colName = if (prefix == null) f.name else prefix + "." + f.name

                f.dataType match {
                    case st: StructType => flattenSchema(st, colName)
                    case _ => Array(col(colName))
                }
            })
        }

        // https://stackoverflow.com/questions/37471346/automatically-and-elegantly-flatten-dataframe-in-spark-sql
        val renamedCols: Array[Column] = flattenSchema(df.schema)
            .map(name => col(name.toString).as(name.toString.replace(".", "_")))
        df.select(renamedCols: _*)
    }

    def deletePath(path: String): Unit ={
        s"hdfs dfs -rm -r -skipTrash $path".!
    }

    def writeToFile(path:String, data:String): Unit = {
        val conf = new Configuration
        Control.using(FileSystem.get(URI.create(path), conf)){ fs =>
            Control.using(fs.create(new Path(path))){ f =>
                f.write(data.getBytes)
            }
        }
    }

    def readFileFully(uri: String): String = {
        val conf = new Configuration
        val fs = FileSystem.get(URI.create(uri), conf)
        Control.using(fs.open(new Path(uri))){
            inputStream => IOUtils.toString(inputStream, "UTF-8")
        }
    }

    def isEmpty(path: String): Boolean = {
        val conf = new Configuration
        Control.using(FileSystem.get(URI.create(path), conf)){ fs =>
            val p = new Path(path)
            if(fs.exists(p)){
                if(fs.getContentSummary(p).getFileCount > 0){
                    return false
                }
            }
            return true
        }
    }

    def exists(path:String): Boolean = {
        val conf = new Configuration
        Control.using(FileSystem.get(URI.create(path), conf)){ fs =>
            val p = new Path(path)
            fs.exists(p)
        }
    }

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
