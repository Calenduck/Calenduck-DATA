package com.competition

import com.competition.utils.DateUtils.changePattern
import com.competition.utils.SparkUtils.getSparkSession
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.{col, explode, udf}

import java.time.LocalDate

case class nameWithMt20ID(name:String, mt20id:String)



object saveShowDetailsJob {
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



    var df : DataFrame = spark.emptyDataset[nameWithMt20ID].toDF()
    mt20ids.foreach{mt20id=>
      val showDetail : DataFrame =
        getShowDetailJob.getShowDetail(spark, mt20id)
          .withColumn("names",splitUdf(col("prfcast")))

      val tmpDf = showDetail.select(explode($"names").alias("name"), col("mt20id"))

      df = df.unionAll(tmpDf)
      Thread.sleep(600)
    }
    df.repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("compression", "snappy")
      .saveAsTable("b_competition.name_with_mt20id")

      //
      //.parquet("nameWithMt20ID")
  }
}
