package com.yss.scala.util

import org.apache.spark.sql.SparkSession

object XMLReader {

  /**
    * 读取XML文件，解析成Row类型的RDD
    * @param path xml文件路径
    * @param sparkSession
    * @return
    */
  def readXML(path:String, sparkSession: SparkSession) = {
    sparkSession.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "Security")
      .load(path)
      .rdd
  }



}
