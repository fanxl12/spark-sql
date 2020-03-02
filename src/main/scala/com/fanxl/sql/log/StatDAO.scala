package com.fanxl.sql.log

import java.sql.{Connection, PreparedStatement}

import scala.collection.mutable.ListBuffer

/**
 * @description 各个维度统计的DAO操作
 * @author: fanxl 
 * @date: 2020/3/2 0002 21:51
 *
 */
object StatDAO {

  /**
   * 批量保存DayVideoAccessStat到数据库
   * @param list
   */
  def insertDayVideoAccessTopN(list: ListBuffer[DayVideoAccessStat]) = {


    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {
      connection = MySQLUtils.getConnection()

      connection.setAutoCommit(false) //设置手动提交

      val sql = "insert into day_video_access_topn_stat(day,cms_id,times) values (?,?,?) "
      pstmt = connection.prepareStatement(sql)

      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setLong(3, ele.times)

        pstmt.addBatch()
      }
      pstmt.executeBatch() // 执行批量处理
      connection.commit() //手工提交
    }catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtils.release(connection, pstmt)
    }

  }

}
