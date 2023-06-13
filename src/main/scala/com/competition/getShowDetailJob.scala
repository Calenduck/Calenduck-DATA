package com.competition

import com.competition.utils.SparkUtils.getSparkSession
import com.databricks.spark.xml._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.explode

import scala.xml.XML

object getShowDetailJob {
  def getShowDetail(spark:SparkSession, mt20id:String): DataFrame = {
    import spark.implicits._

    val url = s"https://www.kopis.or.kr/openApi/restful/pblprfr/$mt20id/?service=9f9df49e424544f4bdf729f203170623" // Replace with your URL
    val xmlString = scala.io.Source.fromURL(url).mkString
    val xmlData = XML.loadString(xmlString).toString

    val df = spark.read
      .format("com.databricks.spark.xml")
      .option("rootTag", "dbs")
      .option("rowTag", "db")
      .xml(spark.createDataset(Seq(xmlData)))

    val explodeDf = df.select("db.*","*")
    explodeDf
  }
}
