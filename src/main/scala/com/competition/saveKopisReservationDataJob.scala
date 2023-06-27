package com.competition

import com.competition.utils.SparkUtils.getSparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.lit

import java.util.Properties

case class kopisReservationData(
                       rank_co:String
                      ,pblprfr_nm:String
                      ,genre_nm:String
                      ,pblprfr_begin_de:String
                      ,pblprfr_end_de:String
                      ,area_nm:String
                      ,pblprfr_place_nm:String
                      ,rdnmadr_nm:String
                      ,lc_la:String
                      ,lc_lo:String
                     )

object saveKopisReservationDataJob {
  def main(args: Array[String]): Unit = {
    val spark = getSparkSession("saveKopis")
    import spark.implicits._
    // Configure the connection properties
    val jdbcHostname = "competition-admin.cjyqslqcsafp.ap-northeast-2.rds.amazonaws.com"
    val jdbcPort = 3306
    val jdbcDatabase = "competition"
    val jdbcUsername = "competition"
    val jdbcPassword = "!!Gg794613"

    val connectionProperties = new Properties()
    connectionProperties.put("user", jdbcUsername)
    connectionProperties.put("password", jdbcPassword)

    val jdbcUrl = s"jdbc:mysql://$jdbcHostname:$jdbcPort/$jdbcDatabase"

    val connection = java.sql.DriverManager.getConnection(jdbcUrl, connectionProperties)
    val statement = connection.createStatement()
    val tableName = "kopis_reservation"
    val createTableQuery =
      s"""CREATE TABLE IF NOT EXISTS $tableName (
         |  rank_co int,
         |  p_ymd varchar(255),
         |  pblprfr_nm varchar(255),
         |  genre_nm varchar(255),
         |  pblprfr_begin_de varchar(255),
         |  pblprfr_end_de varchar(255),
         |  area_nm varchar(255),
         |  pblprfr_place_nm varchar(255),
         |  rdnmadr_nm varchar(255),
         |  lc_la double,
         |  lc_lo double,
         |  PRIMARY KEY (rank_co, p_ymd)
         |)
         |""".stripMargin
    statement.execute(createTableQuery)


    for (i <- 1 until 6) {
      val num = f"$i%02d"
      val df = spark.read
        .options(Map("inferSchema" -> "true", "header" -> "true"))
        .csv(s"3rd-party-data/kopis_reservation/KC*2023$num.csv")
      val rdd = df.as[kopisReservationData].rdd
      val kopisReservationDF = spark.createDataFrame(rdd).withColumn("p_ymd", lit(s"2023$num"))

      kopisReservationDF.show
      kopisReservationDF.write
        .mode(SaveMode.Append)
        .jdbc(jdbcUrl, tableName, connectionProperties)
    }
  }
}
