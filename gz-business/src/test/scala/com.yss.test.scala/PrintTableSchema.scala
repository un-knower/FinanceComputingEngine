package com.yss.test.scala

import java.io.File

import com.yss.scala.util.{DateUtils, Util}
import com.yss.test.scala.PrintTableSchema.TABLE_TYPE.TABLE_TYPE
import org.apache.spark.sql.SparkSession

/**
  * @auther: lijiayan
  * @date: 2018/11/2
  * @desc: 用于打印hdfs上的csv，mysql，oracle数据库里面表的schema
  */
object PrintTableSchema {

  object TABLE_TYPE extends Enumeration {
    type TABLE_TYPE = Value
    val HDFS: PrintTableSchema.TABLE_TYPE.Value = Value("HDFS")
    val ORACLE: PrintTableSchema.TABLE_TYPE.Value = Value("Oracle")
    val MYSQL: PrintTableSchema.TABLE_TYPE.Value = Value("MySQL")
  }

  //要使用的表在hdfs中的路径
  private val TABLE_HDFS_PATH = "hdfs://192.168.102.120:8020/yss/guzhi/basic_list/"

  private val ZC_TABLE = "LSETLIST"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    printSchema(spark, "CSSYSYJLV", TABLE_TYPE.ORACLE)
    spark.stop()
  }


  def printSchema(spark: SparkSession, tableName: String, tableType: TABLE_TYPE) = {
    tableType match {
      case TABLE_TYPE.HDFS => printHDFSSchema(spark, tableName)
      case TABLE_TYPE.ORACLE => printOracleSchema(spark, tableName)
      case TABLE_TYPE.MYSQL => printMySQLSchema(spark, tableName)
      case _ => throw new IllegalArgumentException("所给类型不支持")
    }

  }


  def printHDFSSchema(spark: SparkSession, tableName: String): Unit = {
    Util.readCSV(getTableDataPath(tableName), spark, header = true, sep = "\t").toDF().printSchema()
  }

  def printOracleSchema(spark: SparkSession, tableName: String) = {
    spark.read.format("jdbc")
      .options(
        Map(
          "url" -> "jdbc:oracle:thin:@192.168.102.68:1521:orcl",
          "user" -> "hadoop",
          "password" -> "hadoop",
          "driver" -> "oracle.jdbc.driver.OracleDriver",
          "dbtable" -> tableName
        )
      ).load().printSchema()
  }

  def printMySQLSchema(spark: SparkSession, tableName: String) = {
    spark.read.format("jdbc")
      .options(
        Map(
          "url" -> "jdbc:mysql://192.168.102.120:3306/JJCWGZ",
          "user" -> "root",
          "password" -> "root1234",
          "driver" -> "com.mysql.jdbc.Driver",
          "dbtable" -> tableName
        )
      ).load().printSchema()
  }

  /**
    * 根据表名获取表在hdfs上对应的路径
    *
    * @param tName :表面
    * @return
    */
  def getTableDataPath(tName: String): String = {
    val date = DateUtils.formatDate(System.currentTimeMillis())
    TABLE_HDFS_PATH + date + File.separator + tName
  }

}
