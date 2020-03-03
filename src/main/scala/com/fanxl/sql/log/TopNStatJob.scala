package com.fanxl.sql.log

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

/**
 * @description TopN的统计分析操作
 * @author: fanxl 
 * @date: 2020/3/1 0001 21:12
 *
 */
object TopNStatJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TopNStatJob")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled","false")
      .master("local[2]").getOrCreate()

    val logDF = spark.read.load("file:///E:\\vagrant\\hadoop001\\labs\\log\\clean")

    //最受欢迎的TopN课程
//    videoTopN(spark, logDF)
    // 按照地市进行统计TopN课程
//    cityTopN(spark, logDF)

    //按照流量进行统计
    videoTrafficsTopNStat(spark, logDF)

    spark.stop()
  }

  /**
   * 按照流量进行统计
   * @param spark
   * @param logDF
   */
  def videoTrafficsTopNStat(spark: SparkSession, logDF: DataFrame) = {
    //使用DataFrame方式进行统计
    import spark.implicits._
    val trafficAccessTopNDF = logDF.filter($"day" === "20170511" && $"cmsType" === "video")
      .groupBy("day", "cmsId")
      .agg(sum("traffic").as("traffics"))
      .orderBy($"traffics".desc)

    // 将统计的结果写入到MySql中
    try {
      trafficAccessTopNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoTrafficsStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val traffics = info.getAs[Long]("traffics")

          list.append(DayVideoTrafficsStat(day, cmsId, traffics))
        })

        StatDAO.insertTrafficVideoCityAccessTopN(list)
      })
    }catch {
      case e: Exception => e.printStackTrace()
    }
  }

  /**
   * 按照地市进行统计TopN课程
   * @param spark
   * @param logDF
   */
  def cityTopN(spark: SparkSession, logDF: DataFrame) = {
    //使用DataFrame方式进行统计
    import spark.implicits._
    val cityAccessTopNDF = logDF.filter($"day" === "20170511" && $"cmsType" === "video")
      .groupBy("day", "city", "cmsId")
      .agg(count("cmsId").as("times"))

    //window 函数在spark sql的使用
    val top3DF = cityAccessTopNDF.select(
      cityAccessTopNDF("day"),
      cityAccessTopNDF("city"),
      cityAccessTopNDF("cmsId"),
      cityAccessTopNDF("times"),
      row_number().over(Window.partitionBy(cityAccessTopNDF("city"))
          .orderBy(cityAccessTopNDF("times").desc)
      ).as("times_rank")
    ).filter("times_rank <= 3")

    // 将统计的结果写入到MySql中
    try {
      top3DF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoCityAccessStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val times = info.getAs[Long]("times")
          val city = info.getAs[String]("city")
          val timesRank = info.getAs[Int]("times_rank")

          list.append(DayVideoCityAccessStat(day, cmsId, city, times, timesRank))
        })

        StatDAO.insertDayVideoCityAccessTopN(list)
      })
    }catch {
      case e: Exception => e.printStackTrace()
    }
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

    // 将统计的结果写入到MySql中
    try {
      videoAccessTopNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoAccessStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val times = info.getAs[Long]("times")

          list.append(DayVideoAccessStat(day, cmsId, times))
        })

        StatDAO.insertDayVideoAccessTopN(list)
      })
    }catch {
      case e: Exception => e.printStackTrace()
    }
  }

}
