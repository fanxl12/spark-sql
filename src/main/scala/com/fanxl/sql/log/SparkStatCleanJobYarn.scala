package com.fanxl.sql.log

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * @description 使用Spark完成我们的数据的第二步清洗操作: 运行在Yarn之上
 * @author: fanxl 
 * @date: 2020/3/1 0001 16:27
 *
 */
object SparkStatCleanJobYarn {

  def main(args: Array[String]): Unit = {
    if(args.length !=2) {
      println("Usage: SparkStatCleanJobYARN <inputPath> <outputPath>")
      System.exit(1)
    }

    val Array(inputPath, outputPath) = args

    val sparkSession = SparkSession.builder().getOrCreate()

    val accessRDD = sparkSession.sparkContext.textFile(inputPath)

    //RDD ==> DF
    val accessDF = sparkSession.createDataFrame(accessRDD.map(x => AccessConvertUtil.parseLog(x)), AccessConvertUtil.struct)

    accessDF.coalesce(1).write.format("parquet")
      .mode(SaveMode.Overwrite)
      .partitionBy("day")
      .save(outputPath)

    sparkSession.stop()
  }

}
