package com.fanxl.sql.log

/**
 * 每天每个城市视频类课程访问量
 */
case class DayVideoCityAccessStat(day:String, cmsId:Long, city:String,times:Long,timesRank:Int)