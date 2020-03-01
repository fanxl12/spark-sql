package com.fanxl.sql

import org.apache.spark.sql.SparkSession

/**
 * SparkSession的使用
 */
object SparkSessionApp {

  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("SparkSessionApp")
      .master("local[2]").getOrCreate()

    val people = spark.read.json("file:///E:\\vagrant\\hadoop001\\labs\\persion.json")
    people.show()

    spark.stop()
  }
}
