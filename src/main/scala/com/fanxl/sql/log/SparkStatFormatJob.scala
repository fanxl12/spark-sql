package com.fanxl.sql.log

import org.apache.spark.sql.SparkSession

/**
 * @description 第一步清洗：抽取出我们所需要的指定列的数据
 * @author: fanxl 
 * @date: 2020/3/1 0001 13:17
 *
 */
object SparkStatFormatJob {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("SparkStatFormatJob")
      .master("local[2]").getOrCreate()

    val access = sparkSession.sparkContext.textFile("file:///E:\\vagrant\\hadoop001\\labs\\10000_access.log")

//    access.take(100).foreach(println)

    access.map(item => {
      val splits = item.split(" ")
      val ip = splits(0)
      val time = splits(3) + " " + splits(4)
      val url = splits(11).replace("\"", "")
      val traffic = splits(9)
      DateUtils.parse(time) + "\t" + url + "\t" + traffic + "\t" + ip
    }).saveAsTextFile("file:///E:\\vagrant\\hadoop001\\labs\\output\\")

    sparkSession.stop()
  }

}
