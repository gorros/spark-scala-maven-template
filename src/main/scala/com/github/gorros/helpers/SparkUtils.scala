package com.github.gorros.helpers

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col,from_json}
import org.apache.spark.sql.types.StructType

object SparkUtils {
    implicit class DataFrameUtils(df: DataFrame) {

        def renameColumns(f: String => String): DataFrame =
            df.columns.foldLeft(df)((tempDf, c) => tempDf.withColumnRenamed(c, f(c)))

        def dropColumns(f: String => Boolean): DataFrame =
            df.columns.foldLeft(df)((tempDf, c) => if (f(c)) tempDf.drop(c) else tempDf)

        def camelCaseToSnakeCaseColumns: DataFrame =
            renameColumns(_.replaceAll("([A-Z]+)", "_$1").toLowerCase.stripPrefix("_"))

        def setAllColumnsNullable(nullable: Boolean): DataFrame = {
            df.sqlContext.createDataFrame(df.rdd, StructType(df.schema.map(_.copy(nullable = nullable))))
        }

        def lowerCaseColumns: DataFrame = renameColumns(_.toLowerCase)

        def trimColumns: DataFrame = renameColumns(_.trim)

        def flatten: DataFrame = {
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

        def createTableFromDf(db: String, table: String, path: String,
                              partitionColumn: Option[String] = None): Unit = {
            // Create empty table
            if (partitionColumn.isDefined) {
                df.where("1 = 0").write
                    .partitionBy(partitionColumn.get)
                    .mode(SaveMode.Append)
                    .format("parquet")
                    .option("path", path)
                    .saveAsTable(s"$db.$table")
            } else {
                df.where("1 = 0").write
                    .mode(SaveMode.Append)
                    .format("parquet")
                    .option("path", path)
                    .saveAsTable(s"$db.$table")
            }
        }

        // consider other name for method
        def schemaIsChanged(db: String, table: String): Boolean = {
            val tableColumns = df.sparkSession.sql(s"DESCRIBE $db.$table").select("col_name")
                .collect().map(r => r.getString(0)).toSet
            df.columns.toSet  != tableColumns
        }
    }

    implicit class SparkSessionUtils(ss: SparkSession) {
        def updatePartitions(db: String, table: String): Unit = {
            ss.sql(s"MSCK REPAIR TABLE $db.$table")
        }

        def tableExists(db: String, table: String): Boolean = {
            ss.catalog.tableExists(db, table)
        }

        def dropTable(db: String, table: String): Unit = {
            ss.sql(s"DROP TABLE IF EXISTS $db.$table")
        }

        def jsonRDDToDF(rdd: RDD[String], schema: StructType): DataFrame = {
            import ss.implicits._
            rdd.toDF("json").select(from_json('json, schema).as("json")).select("json.*")
        }
    }
}

