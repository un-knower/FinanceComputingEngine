package com.yss.scala.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @author yupan
  *         2018-08-27 11:36
  *
  */
object sparktest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("yss").getOrCreate()
    val sc = spark.sparkContext

    val data: RDD[String] =sc.parallelize(Array("1","2","3","4","5","6","7","8","9"),3)
    data.saveAsTextFile("C:\\Users\\dell\\Desktop\\mm")
    sc.stop()
  }
}
