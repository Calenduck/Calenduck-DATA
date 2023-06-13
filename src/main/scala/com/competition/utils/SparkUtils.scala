package com.competition.utils

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SaveMode, SparkSession}

import java.util

object SparkUtils {

  def getSparkSession(appName:String): SparkSession ={
    SparkSession.builder
      .enableHiveSupport()
      .appName(appName)
      .config("spark.sql.session.timeZone", "Asia/Seoul")
      //.master("local[*]")
      .getOrCreate()
  }

  def fieldConstructor(typeMaps : Map[String,org.apache.spark.sql.types.DataType], order:Array[String]): StructType = {
    val fields = order.map{
      v =>
        val name = v
        val structType = typeMaps(v)
        StructField(name, structType)
    }
    StructType(fields)
  }

  def typeCasterWithMap(df: DataFrame, schemaWithTypeMap: Map[String, org.apache.spark.sql.types.DataType]): DataFrame = {
    var newDF: DataFrame = df
    schemaWithTypeMap.foreach{ el =>
      newDF = newDF.withColumn(el._1, col(el._1).cast(el._2))
    }
    newDF
  }

  def saveDF(spark:SparkSession,
             df:DataFrame,
             location:String,
             db_name:String,
             table_name:String,
             p_ymd:String,
             method:String = "default"): Unit = {
    if (method == "coalesce"){
      df.coalesce(1)
        .write
        .option("compression", "snappy")
        .mode(SaveMode.Append)
        .parquet(location)
    }else{
      df.repartition(1)
        .write
        .option("compression", "snappy")
        .mode(SaveMode.Append)
        .parquet(location)
    }


    spark.sql(s"ALTER TABLE $db_name.$table_name ADD IF NOT EXISTS PARTITION (p_ymd='$p_ymd')")
    println(s"$db_name.$table_name\n\n")
  }

  def getMySQL(spark: SparkSession, targetEpDB:String, secretsMap: util.HashMap[String, String]): DataFrameReader = {
    val upperDB = targetEpDB
    var lowerDB = ""
    if (upperDB.startsWith("GLOBAL")) {
      lowerDB = upperDB.split("_")(1).toLowerCase
      print(lowerDB)
    } else {
      lowerDB = upperDB.toLowerCase
      print(lowerDB)
    }

    val ep = secretsMap.get(f"RDS_${upperDB}_DP_EP")
    val usr = secretsMap.get(f"RDS_${upperDB}_DP_USER")
    val pw = secretsMap.get(f"RDS_${upperDB}_DP_PW")

    val host = s"jdbc:mysql://$ep:3306/$lowerDB?serverTimezone=Asia/Seoul&useSSL=false"

    val conn = spark.read.format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", host)
      .option("user", usr)
      .option("password", pw)

    conn
  }

}
