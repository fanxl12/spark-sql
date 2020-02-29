package com.fanxl.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description com.fanxl.sql.SQLContextApp
 * @author: fanxl 
 * @date: 2020/2/28 0028 13:50
 *
 */
object SQLContextApp {

  def main(args: Array[String]): Unit = {

    val path = args(0)

    //1)创建相应的Context
    val sparkConf = new SparkConf()
//    sparkConf.setAppName("SQLContextApp").setMaster("local[2]")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    //2)相关的处理
    val people = sqlContext.read.format("json").load(path)
    people.printSchema()
    people.show()

    //3)关闭资源
    sc.stop()
  }

}
