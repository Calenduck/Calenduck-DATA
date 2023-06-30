package com.competition

import com.competition.utils.SparkUtils.getSparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.lit

import java.util.Properties

case class dmkMusicalTOP10(
                            seq_no:String
                            ,all_kwrd_rank_co:String
                            ,srchwrd_nm:String
                            ,upper_ctgry_nm:String
                            ,lwprt_ctgry_nm:String
                            ,genre_nm:String
                            ,mobile_sccnt_value:String
                            ,pc_sccnt_value:String
                            ,sccnt_sm_value:String
                            ,sccnt_de:String
                          )

object saveDMKMusicalTOP10Job {
  def main(args: Array[String]): Unit = {
    val spark = getSparkSession("saveDMKMusicalTOP10")
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
    val tableName = "dmk_musical_top10"
    val createTableQuery =
      s"""CREATE TABLE IF NOT EXISTS $tableName (
         |  seq_no int,
         |  all_kwrd_rank_co int,
         |  srchwrd_nm varchar(255),
         |  upper_ctgry_nm varchar(255),
         |  lwprt_ctgry_nm varchar(255),
         |  genre_nm varchar(255),
         |  mobile_sccnt_value int,
         |  pc_sccnt_value int,
         |  sccnt_sm_value int,
         |  sccnt_de varchar(255),
         |  PRIMARY KEY (seq_no)
         |)
         |""".stripMargin
    statement.execute(createTableQuery)


    for (i <- 1 until 6) {
      val num = f"$i%02d"
      val df = spark.read
        .options(Map("inferSchema" -> "true", "header" -> "true"))
        .csv(s"3rd-party-data/musical_top10/*.csv")
      val rdd = df.as[kopisReservationData].rdd
      val dmkMusicalTOP10 = spark.createDataFrame(rdd)

      dmkMusicalTOP10.show
      dmkMusicalTOP10.write
        .mode(SaveMode.Append)
        .jdbc(jdbcUrl, tableName, connectionProperties)
    }
  }
}
