package com.competition

import com.competition.utils.SparkUtils.getSparkSession

object searchArtistJob {
  def main(args: Array[String]): Unit = {
    val spark = getSparkSession("search")
    val nameWithShowTitleDf = spark.read.parquet("nameWithShowTitle")
    nameWithShowTitleDf.createOrReplaceTempView("nameWithShowTitle")

    val name=args(0)
    spark.sql(
      s"""
         |select *
         |from nameWithShowTitle
         |where name='$name'
         |""".stripMargin).show
  }
}
