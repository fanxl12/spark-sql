package com.fanxl.sql.log

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * @description TopN的统计分析操作
 * @author: fanxl 
 * @date: 2020/3/1 0001 21:12
 *
 */
object TopNStatJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TopNStatJob")
      .master("local[2]").getOrCreate()

    val logDF = spark.read.load("file:///E:\\vagrant\\hadoop001\\labs\\log\\clean")

    videoTopN(spark, logDF)

    spark.stop()
  }

  /**
   * 最受欢迎的TopN课程
   * @param spark
   * @param logDF
   */
  def videoTopN(spark: SparkSession, logDF: DataFrame) = {
    //使用DataFrame方式进行统计
//    import spark.implicits._
//    val videoAccessTopNDF = logDF.filter($"day" === "20170511" && $"cmsType" === "video")
//      .groupBy("day", "cmsId").agg(count("cmsId").as("times"))
//      .sort($"times".desc)

    //使用SQL方式进行统计
    logDF.createOrReplaceTempView("access_logs")
    val videoAccessTopNDF = spark.sql("select day, cmsId, count(1) as times from access_logs " +
      "where day = '20170511' and cmsType = 'video'" +
      "group by day, cmsId order by times desc")

    videoAccessTopNDF.show(false)
  }

}
