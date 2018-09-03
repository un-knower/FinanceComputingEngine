package com.yss.test.scala

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Test1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ShenJiaoSuo").master("lcoal[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._


  }
}
