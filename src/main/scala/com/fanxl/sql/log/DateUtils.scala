package com.fanxl.sql.log

import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

/**
 * @description 日期时间解析工具类:
 *             注意：SimpleDateFormat是线程不安全
 * @author: fanxl 
 * @date: 2020/3/1 0001 16:15
 *
 */
object DateUtils {

  //输入文件日期时间格式
  //10/Nov/2016:00:01:02 +0800
  val YYYYMMDDHHMM_TIME_FORMAT = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)

  //目标日期格式
  val TARGET_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  /**
   * 获取输入日志时间：long类型
   * @param time [10/Nov/2016:00:01:02 +0800]
   * @return
   */
  def getTime(time: String) = {
    try {
      YYYYMMDDHHMM_TIME_FORMAT.parse(time.substring(time.indexOf("[") + 1, time.indexOf("]"))).getTime
    }catch {
      case e: Exception => {
        0L
      }
    }
  }


  /**
   *  获取时间：yyyy-MM-dd HH:mm:ss
   * @param time [10/Nov/2016:00:01:02 +0800]
   * @return
   */
  def parse(time: String) = {
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

  def main(args: Array[String]): Unit = {
    println(parse("[10/Nov/2016:00:01:02 +0800]"))
  }

}
