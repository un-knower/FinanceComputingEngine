package com.yss.scala.util

import java.io.FileInputStream
import java.sql.{Connection, DriverManager}
import java.util.Properties

object MysqlUtil {

  /**
    * mysql数据库连接方法
    * 通过配置文件连接数据库
    */
  def getConn(): Connection = {
    val path = "mysqlConnectionProperties.properties"
    val properties = new Properties()
    properties.load(new FileInputStream(path))
    val url = properties.getProperty("url")
    val database = properties.getProperty("dataBase")
    val userName = properties.getProperty("userName", "root")
    val password = properties.getProperty("password")
    val conn:Connection = DriverManager.getConnection(url+database, userName, password)
    conn
  }
}
