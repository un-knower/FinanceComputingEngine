package com.yss.guzhi.sparkbuss

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * @author : 张海绥
  * @version : 2018-9-4
  *  describe: 期货成交明细
  *  目标文件：09000211trddata20180420.txt
  *  目标表：QHCJMX
  */
object ShenJiaoSuo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("ShenJiaoSuo").master("lcoal[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._

    //读取TSV格式的目标文件
    val frame: DataFrame = spark.read.format("tsv")
      .option("delimiter", "\t") //          指定分隔符
      //.option("inferSchema", "true") //          推断数据类型
      .option("header", "false") //          没有表头false
      .load("C:\\Users\\YZM\\Desktop\\test.tsv")
    frame.show()

  }
}
