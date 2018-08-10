package com.yss.scala.guzhi

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
  * imcexchangerate.xml数据接口
  *
  * @author zhangyl
  * @date 2018/8/7
  *
  * 按照要求的字段格式将源文件imcexchangerate.xml文件的字段
  * 写入新的数据文件中
  */
object ImcExchangeRate {
  def main(args: Array[String]): Unit = {

    val sqlContext = SparkSession.builder().master("local[*]").getOrCreate()

    val inputPath = "F:/work/evaluation/test_data/imcexchangerate.xml"

    val outputPath = "F:/work/evaluation/test_data/ImcExchangeRateCsv"

    //定义需要存入表格字段格式
    val fieldSchema = StructType(Array(
      StructField("sjlx", StringType, true),
      StructField("mrhl", DoubleType, true),
      StructField("mlhl", DoubleType, true),
      StructField("zjhl", DoubleType, true),
      StructField("bz", StringType, true),
      StructField("rq", LongType, true),
      StructField("BYs", StringType, true)
    ))

    //写出表字段与原始数据对应方式
    val rowRDD = XMLReader.readXML(inputPath, sqlContext).map(row => {
      val BidRate = row(0).toString.toDouble
      val FromCurrency = row(1).toString.trim()
      val MidPointRate = row(2).toString.toDouble
      val OfferRate = row(3).toString.toDouble
      val date = new Date().getTime
      Row(
        "102",
        BidRate,
        OfferRate,
        MidPointRate,
        FromCurrency,
        date,
        " "
      )
    })

    //数据写出到表
    sqlContext.createDataFrame(rowRDD, fieldSchema)
      .write
      .format("jdbc")
      .option("url", "jdbc:mysql://192.168.102.119:3306/JJCWGZ?useUnicode=true&characterEncoding=utf8")
      .option("dbtable", "imcexchangerate")
      .option("user", "test01")
      .option("password", "test01")
      .mode(SaveMode.Append)
      .save()

    sqlContext.stop()
  }
}
