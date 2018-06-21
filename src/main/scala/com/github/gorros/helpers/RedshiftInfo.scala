package com.github.gorros.helpers

case class RedshiftInfo(host: String,
                        port:String,
                        db: String,
                        table: String,
                        user: String,
                        password: String,
                        iamRole: String,
                        query: String,
                        tempDir: String,
                        jdbcURL:String,
                        sql: String,
                        tempFormat: String = "CSV GZIP",
                        distStyle: String = "EVEN",
                        distKey: Option[String] = None,
                        sortKey: Option[String] = None)