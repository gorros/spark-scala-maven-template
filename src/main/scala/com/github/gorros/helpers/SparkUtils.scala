package com.github.gorros.helpers


import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.col
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
    }
}

