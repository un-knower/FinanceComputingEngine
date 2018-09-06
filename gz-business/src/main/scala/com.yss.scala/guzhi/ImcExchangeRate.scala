package com.yss.scala.guzhi

import java.util.Date

import com.yss.scala.util.Util
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/**
  * imcexchangerate.xml数据接口
  *
  * @author zhangyl
  *
  * 按照要求的字段格式将源文件imcexchangerate.xml文件的字段
  * 写入新的数据文件中
  */
object ImcExchangeRate {
  def main(args: Array[String]): Unit = {

    val sqlContext = SparkSession.builder()
      .appName("imcexchangerate")
      .getOrCreate()

    //val inputPath = "F:/work/evaluation/test_data/imcexchangerate.xml"
    val tableName ="imcexchangerate.xml"

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
    val rowRDD = Util.readXML(Util.getInputFilePath(tableName), sqlContext).map(row => {
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
    val df = sqlContext.createDataFrame(rowRDD, fieldSchema)
    Util.outputMySql(df,"imcexchangerate")

    sqlContext.stop()
  }
}
