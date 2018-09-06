package com.yss.test.scala

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.sql.SparkSession

object MyTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("yss").getOrCreate()
    val sc = spark.sparkContext
    val csb = sc.
//      hadoopFile("D:\\赢时胜资料\\估值核算\\基本数据\\参数tsv.tsv",
        hadoopFile("D:\\赢时胜资料\\估值核算\\基本数据\\参数.csv",
        classOf[TextInputFormat], classOf[LongWritable], classOf[Text])
      .map(pair => new String(pair._2.getBytes, 0, pair._2.getLength, "GBK"))

    val csbMap = csb.map(row => {
      val fields = row.replaceAll(""""""","").split(",")
        print(fields)
      val key = fields(0)//参数名
      val value = fields(1) //参数值
      (key, value)
    }).collectAsMap()
    println(csbMap)  //---Map("117深圳中登计算数据费用处理模式值" -> "0", "117积数法计息自动产生付息凭证" -> "0"）
    println(csbMap("117深圳中登计算数据费用处理模式值"))  //获取不到这个值？？？
  }
}
