package com.spark

import java.io.FileInputStream
import java.net.InetAddress
import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Util {

  /**
    * 读取XML文件，解析成Row类型的RDD
    *
    * @param path xml文件路径
    * @param sparkSession
    * @return
    */
  def readXML(path: String, sparkSession: SparkSession) = {
    sparkSession.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "Security")
      .load(path)
      .rdd
  }

  /**
    * 读取csv格式的数据
    *
    * @param path
    * @param sparkSession
    * @return
    */
  def readCSV(path: String, sparkSession: SparkSession, header: Boolean = true) = {
    sparkSession.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "false")
      .option("header", header)
      .load(path)
  }

  /**
    * 获取文件输入路径
    *
    * @param fileName 文件输入名
    */
  def getInputFilePath(fileName: String) = {
    val hdfsDir = "hdfs://192.168.102.120:8020/yss/guzhi/"
    val inputFilePath = hdfsDir + fileName
    inputFilePath
  }

  /**
    * 获取文件输入路径
    *
    * @param fileName 文件输入名
    */
  def getDailyInputFilePath(fileName: String) = {
    val today = DateUtils.getToday(DateUtils.yyyyMMdd)
    val hdfsDir = "hdfs://192.168.102.120:8020/yss/guzhi/basic_list/" + today + "/"
    val inputFilePath = hdfsDir + fileName
    inputFilePath
  }

  /**
    * 用于测试，获取文件本地输入路径
    *
    * @param fileName 文件输入名
    */
  def getInputLocalFilePath(fileName: String) = {
    val str: String = (InetAddress.getLocalHost()).getHostName()
    val userName = str.split("-")(0)
    val hdfsDir = "C:\\Users\\" + userName + "\\Desktop\\"
    val inputFilePath = hdfsDir + fileName
    inputFilePath
  }
  /**
    * 将dataFrame类型数据结果输出到MySql<192.168.102.119>数据库J<JJCWGZ>中
    *
    * @param DF        数据结果
    * @param tableName 输出的表名
    */
  def outputMySql(DF: DataFrame, tableName: String) = {
    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "root1234")
    properties.setProperty("driver", "com.mysql.jdbc.Driver") //这句话一定要加上不然报错缺少jdbc驱动
    DF.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://192.168.102.120/JJCWGZ?useUnicode=true&characterEncoding=utf8", tableName, properties)
  }

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
    val conn: Connection = DriverManager.getConnection(url + database, userName, password)
    conn
  }

}
