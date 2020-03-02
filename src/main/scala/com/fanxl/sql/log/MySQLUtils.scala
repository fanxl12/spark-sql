package com.fanxl.sql.log

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.calcite.prepare.Prepare

/**
 * @description MySQL操作工具类
 * @author: fanxl 
 * @date: 2020/3/2 0002 12:52
 *
 */
object MySQLUtils {

  /**
   *  获取数据库连接
   * */
  def getConnection() = {
    DriverManager.getConnection("jdbc:mysql://localhost:3306/fan_project?user=root&password=fxl421125")
  }

  /**
   * 释放数据库连接等资源
   * @param connection
   * @param pstmt
   */
  def release(connection: Connection, pstmt: PreparedStatement) = {
    try {
      if (pstmt != null) {
        pstmt.close()
      }
    }catch {
      case e : Exception => e.printStackTrace()
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
  }

}
