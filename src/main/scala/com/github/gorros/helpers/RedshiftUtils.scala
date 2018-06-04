package com.github.gorros.helpers


import java.lang.Boolean
import java.sql.{Connection, DriverManager}

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import org.apache.spark.sql._

object RedshiftUtils {

    def getDfFromRedshift(ss: SparkSession, redshiftInfo: RedshiftInfo): DataFrame = {

        val dfReader: DataFrameReader = ss.sqlContext.read
            .format("com.databricks.spark.redshift")
            .option("url", redshiftInfo.jdbcURL)
            .option("user", redshiftInfo.user)
            .option("password", redshiftInfo.password)
            .option("query", redshiftInfo.query)
            .option("tempdir", redshiftInfo.tempDir)


        if (Boolean.getBoolean("debug")) {
            val credentials = new DefaultAWSCredentialsProviderChain().getCredentials
            ss.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", credentials.getAWSAccessKeyId)
            ss.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", credentials.getAWSSecretKey)
            dfReader.option("forward_spark_s3_credentials", "true")
        } else {
            dfReader.option("aws_iam_role", redshiftInfo.iamRole)
        }
        dfReader.load()
    }

    def saveDfToRedshift(df: DataFrame, ss: SparkSession, redshiftInfo: RedshiftInfo): Unit = {

        val dfWriter = df.write.format("com.databricks.spark.redshift")
            .option("url", redshiftInfo.jdbcURL)
            .option("user", redshiftInfo.user)
            .option("password", redshiftInfo.password)
            .option("dbtable", redshiftInfo.table)
            .option("tempdir", redshiftInfo.tempDir)
            .option("tempformat", "CSV GZIP")
            .mode(SaveMode.Append)

        if (Boolean.getBoolean("debug")) {
            val credentials = new DefaultAWSCredentialsProviderChain().getCredentials
            ss.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", credentials.getAWSAccessKeyId)
            ss.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", credentials.getAWSSecretKey)
            dfWriter.option("forward_spark_s3_credentials", "true")
        } else {
            dfWriter.option("aws_iam_role", redshiftInfo.iamRole)
        }
        dfWriter.save()
    }

    def executeRedshiftSQLCheck(jdbc: String, uid: String, pwd: String, sql: String): Boolean = {
        Class.forName("com.amazon.redshift.jdbc4.Driver")
        Control.using(DriverManager.getConnection(jdbc, uid, pwd)){ conn =>
            Control.using(conn.createStatement()){ stmt =>
                val rs = stmt.executeQuery(sql)
                rs.next()
                rs.getBoolean(1)
            }
        }
    }

    def executeRedshiftSQL(jdbc: String, uid: String, pwd: String, sql: String): Unit = {
        Class.forName("com.amazon.redshift.jdbc4.Driver")
        Control.using(DriverManager.getConnection(jdbc, uid, pwd)){ conn =>
            Control.using(conn.createStatement()){ stmt =>
                stmt.execute(sql)
            }
        }
    }

    def createRedshiftConn(redshiftInfo: RedshiftInfo): Connection = {
        Class.forName("com.amazon.redshift.jdbc4.Driver")
        DriverManager.getConnection(redshiftInfo.jdbcURL, redshiftInfo.user, redshiftInfo.password)
    }

}
