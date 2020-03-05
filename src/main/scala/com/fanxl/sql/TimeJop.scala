package com.fanxl.sql

import org.apache.spark.sql.SparkSession

/**
 * @description
 * @author: fanxl 
 * @date: 2020/3/4 0004 14:50
 *
 */
object TimeJop {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TimeJop")
      .master("local[2]").getOrCreate()

    val logDF = spark.read.json("file:///E:\\vagrant\\hadoop001\\labs\\time.json")
    logDF.createOrReplaceTempView("time_logs")

    val logEvent = spark.sql("select user_id, event_id, active_time, " +
      "lag(active_time, 1) over(partition by user_id, active_date order by active_time) as start_time, " +
      "lag(event_id, 1) over(partition by user_id, active_date order by active_time) as start_event_id" +
      " from time_logs")
    logEvent.createOrReplaceTempView("time_log_event")

    spark.sql("select * from time_log_event").show(false)

    spark.sql("select user_id, start_event_id, event_id, start_time, active_time, " +
      "unix_timestamp(active_time) - unix_timestamp(start_time) as dr, " +
      "case when start_event_id = '1065' and event_id = '1066' then unix_timestamp(active_time) - unix_timestamp(start_time) " +
      "else case when unix_timestamp(active_time) - unix_timestamp(start_time) > 600 then 600" +
      " else unix_timestamp(active_time) - unix_timestamp(start_time) end " +
      "end as dd" +
      " from time_log_event " +
      "where start_event_id is not null and (event_id != '1065' or start_event_id != '1066')")
      .show(false)
  }

}
