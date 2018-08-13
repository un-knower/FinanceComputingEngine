package com.yss.scala.guzhi

import java.io.File
import java.util.Properties
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object SPOTMATCH {

  def main(args: Array[String]): Unit = {
  val conf = new SparkConf().setAppName("SPOTMATCH").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val sqlcontext = new SQLContext(sc)
//  val InputPath = "hdfs://nscluster/yss/guzhi/I202911S18061200SPOTMATCH.TXT"
  val InputPath = "C:\\WorkSpace\\Project\\Account\\jiekou\\I202911S18061200SPOTMATCH.TXT"
  val value  = sc.textFile(InputPath).map(x => {
    val InputFile = "I202911S18061200SPOTMATCH.TXT"
      val splits = x.split("[|]")
      Row(splits(0).trim.toString ,
          splits(1).trim.toString ,
          splits(2).trim.toString ,
          splits(3).trim.toString ,
          splits(5).trim.toString ,
          splits(6).trim.toString ,
          splits(7).trim.toString ,
          splits(8).trim.toString ,
          splits(9).trim.toString ,
          splits(10).trim.toString,
          splits(11).trim.toString,
          splits(12).trim.toString,
          InputFile)
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
