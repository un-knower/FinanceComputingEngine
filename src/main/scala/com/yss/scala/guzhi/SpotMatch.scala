package com.yss.scala.guzhi

import java.util.Properties
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author  wfy
  * @version 2018-08-08
  *          描述：现货成交单数据文件
  *          源文件：SPOTMATCH.TXT
  *          结果表：GoldSpotTrade
  */

object SpotMatch {

  def main(args: Array[String]): Unit = {

  val conf = new SparkConf().setAppName("SPOTMATCH").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val sqlcontext = new SQLContext(sc)
//  val InputPath = "hdfs://nscluster/yss/guzhi/I202911S18061200SPOTMATCH.TXT"
  val InputPath = "C:\\WorkSpace\\Project\\Account\\jiekou\\I202911S18061200SPOTMATCH.TXT"
  val value  = sc.textFile(InputPath).map(x => {
      val InputFile = "I202911S18061200SPOTMATCH.TXT"
      val splits = x.split("[|]")
      Row(splits(0).trim.toString ,               //成交编号
          splits(1).trim.toString ,               //买卖方向
          splits(2).trim.toString ,               //客户代码
          splits(3).trim.toString ,               //会员代码
          splits(5).trim.toString ,               //合约代码
          splits(6).trim.toString ,               //成交日期
          splits(7).trim.toString ,               //成交时间
          splits(8).trim.toString ,               //价格
          splits(9).trim.toString ,               //数量
          splits(10).trim.toString,               //系统报单号
          splits(11).trim.toString,               //报单本地编号
          splits(12).trim.toString,               //类型
          InputFile)                              //数据来源文件名称
    })

  val fieldSchema =
      StructType(Array(
      StructField("fcjbh",    StringType, true),
      StructField("FMMFX",    StringType, true),
      StructField("FKHDM",    StringType, true),
      StructField("FMMDM",    StringType, true),
      StructField("FHYDM",    StringType, true),
      StructField("FCJSJ",    StringType, true),
      StructField("FCJSFM",   StringType, true),
      StructField("FCJJG",    StringType, true),
      StructField("FCJSL",    StringType, true),
      StructField("FXTBDH",   StringType, true),
      StructField("FBDBDBH",  StringType, true),
      StructField("FLX",      StringType, true),
      StructField("ffileName",StringType, true)))

  val valueDataFrame = sqlcontext.createDataFrame(value , fieldSchema)

  val prop = new Properties()
  prop.put("driver","com.mysql.jdbc.Driver")
  prop.setProperty("user","root")
  prop.setProperty("password","root1234")
  val url = "jdbc:mysql://192.168.102.119:3306/JJCWGZ"
  valueDataFrame.write.mode(SaveMode.Overwrite).jdbc(url , "GoldSpotTrade" , prop)

  }
}
