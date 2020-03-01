package com.fanxl.sql.log

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * @description 使用Spark完成我们的数据的第二步清洗操作
 * @author: fanxl 
 * @date: 2020/3/1 0001 16:27
 *
 */
object SparkStatCleanJob {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName("SparkStatCleanJob")
      .master("local[2]").getOrCreate()

    val accessRDD = sparkSession.sparkContext.textFile("file:///E:\\vagrant\\hadoop001\\labs\\access.log")

    //RDD ==> DF
    val accessDF = sparkSession.createDataFrame(accessRDD.map(x => AccessConvertUtil.parseLog(x)), AccessConvertUtil.struct)

//    accessDF.printSchema()
    // show(false) 显示的时候，字符串不截取
//    accessDF.show(false)

    accessDF.coalesce(1).write.format("parquet")
      .mode(SaveMode.Overwrite)
      .partitionBy("day")
      .save("file:///E:\\vagrant\\hadoop001\\labs\\log\\clean")

    sparkSession.stop()
  }

}
