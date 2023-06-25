package com.competition

import com.competition.utils.DateUtils.changePattern
import com.competition.utils.SparkUtils.getSparkSession
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.{col, explode, udf}
import java.util.Properties


import java.time.LocalDate

case class nameWithMt20ID(name:String, mt20id:String)
case class detailInfo(mt20id:String
                      ,mt10id:String
                      ,prfnm:String
                      ,fcltynm:String
                      ,prfcast:String
                      ,prfcrew:String
                      ,prfruntime:String
                      ,prfage:String
                      ,entrpsnm:String
                      ,pcseguidance:String
                      ,genrenm:String
                     )

object saveNameWithMt20IDJob {
  val splitString = (x:String) => {
    val result = x.replace(" ","").replace("등","").split(",")
    result
  }

  val splitUdf = udf(splitString)

  def main(args: Array[String]): Unit = {

    val spark = getSparkSession("for competition")
    import spark.implicits._

    val todayDate : LocalDate = LocalDate.now
    val from = changePattern(todayDate,"yyyyMMdd")
    val to = changePattern(todayDate.minusMonths(-3),"yyyyMMdd")

    val showInfos : DataFrame = getShowInfoJob.getShowInfo(spark, from, to)
    val mt20ids = showInfos.select("mt20id").collect.map(v=>v(0).toString)

    var detailInfoDf : DataFrame = spark.emptyDataset[nameWithMt20ID].toDF()
    var nameWithMt20idDf : DataFrame = spark.emptyDataset[nameWithMt20ID].toDF()
    mt20ids.foreach{mt20id=>
      val showDetail : DataFrame =
        getShowDetailJob.getShowDetail(spark, mt20id)
          .withColumn("names",splitUdf(col("prfcast")))

      val tmpDf = showDetail.select(explode($"names").alias("name"), col("mt20id"))

      nameWithMt20idDf = nameWithMt20idDf.unionAll(tmpDf)
      Thread.sleep(600)
    }

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

    // Create the table if it doesn't exist
    var tableName = "name_with_mt20id"
    var createTableQuery =
      s"""CREATE TABLE IF NOT EXISTS $tableName (
         |  name VARCHAR(100),
         |  mt20id VARCHAR(100)
         |)""".stripMargin

    tableName = "detail_info"
    createTableQuery =
      s"""CREATE TABLE IF NOT EXISTS $tableName (
         |mt20id VARCHAR(100),
         |mt10id VARCHAR(100),
         |prfnm VARCHAR(100),
         |fcltynm VARCHAR(100),
         |prfcast VARCHAR(100),
         |prfcrew VARCHAR(100),
         |prfruntime VARCHAR(100),
         |prfage VARCHAR(100),
         |entrpsnm VARCHAR(100),
         |pcseguidance VARCHAR(100),
         |genrenm VARCHAR(100)
         |)""".stripMargin

    val connection = java.sql.DriverManager.getConnection(jdbcUrl, connectionProperties)
    val statement = connection.createStatement()
    statement.execute(createTableQuery)

    // Save the DataFrame to AWS RDS
    nameWithMt20idDf.write
      .mode(SaveMode.Overwrite)
      .jdbc(jdbcUrl, tableName, connectionProperties)

    statement.close()
    connection.close()

    //nameWithMt20idDf.repartition(1)
    //  .write
    //  .mode(SaveMode.Overwrite)
    //  .option("compression", "snappy")
    //  .saveAsTable("b_competition.name_with_mt20id")

  }
}
