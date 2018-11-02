package com.yss.test.scala

import java.io.File

import com.yss.scala.util.{DateUtils, Util}
import org.apache.spark.sql.SparkSession

object PrintTableSchama {

  //要使用的表在hdfs中的路径
  private val TABLE_HDFS_PATH = "hdfs://192.168.102.120:8020/yss/guzhi/basic_list/"

  private val ZC_TABLE = "LSETLIST"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    printSchama(spark,ZC_TABLE)

    spark.stop()
  }


  def printSchama(spark:SparkSession,tableName:String)={
    Util.readCSV(getTableDataPath(tableName),spark,header = true,sep = "\t").toDF().printSchema()
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
