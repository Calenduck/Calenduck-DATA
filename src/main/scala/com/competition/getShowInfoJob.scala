package com.competition

import com.databricks.spark.xml.XmlDataFrameReader
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.explode

import scala.io.Source
import java.net.URL
import scala.xml.XML

object getShowInfoJob {
  def getShowInfo(spark:SparkSession, from:String, to:String): DataFrame = {
    import spark.implicits._

    val apiURL = "http://www.kopis.or.kr/openApi/restful/pblprfr"
    val apiKey = "9f9df49e424544f4bdf729f203170623"
    val params = Map(
      "service" -> apiKey,
      "stdate" -> from,
      "eddate" -> to,
      "cpage" -> "1",
      "rows" -> "100000",
    )
    val queryString = params.map { case (k, v) => s"$k=$v" }.mkString("&")
    val url = new URL(apiURL + "?" + queryString)

    val xmlString = scala.io.Source.fromURL(url).mkString
    val xmlData = XML.loadString(xmlString).toString

    val df = spark.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "db")
      .xml(spark.createDataset(Seq(xmlData)))
      .select(explode($"db"))

    val explodeDf = df.select("col.*","*")
    explodeDf
  }
}
