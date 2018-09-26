package com.spark

import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable


object SparkKafka {
  val SEPARATE1 = "@"
  val SEPARATE2 = ","
  def main(args: Array[String]): Unit = {
    running()

  }

  private def running(): Unit = {
    val spark = SparkSession.builder().appName("sparkDemo").master("local[*]").getOrCreate()
    val sc = new SparkContext()
    val conf = new SparkConf().setAppName("sparkDemo").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))


    //读取基金信息表，并转换成map结构
    val csjjxx = sc.textFile("E:\\WXWork Files\\File\\2018-09\\etl\\data\\CSJJXX_201809051144.csv")
    val value= csjjxx.map(s => {
      val str = s.split(",")
      val fzqlx =str(9)
      val fzqdmetf0 =str(2)
      val fzqdmetf1 = str(3)
      ((fzqlx+SEPARATE1+fzqdmetf0),fzqdmetf1)
    }).collectAsMap()

    val value2 = csjjxx.map(s => {
      val str = s.split(",")
      val fzqlx =str(9)
      val fzqdmetf0 =str(2)
      val fzqdmetf2 = str(4)
      //      val fzqdmetf5 = str(16)
      ((fzqlx+SEPARATE1+fzqdmetf0),fzqdmetf2)
    }).collectAsMap()

    val value3 = csjjxx.map(s => {
      val str = s.split(",")
      val fzqlx =str(9)
      val fzqdmetf0 =str(2)
      val fzqdmetf5 = str(16)
      ((fzqlx+SEPARATE1+fzqdmetf0),fzqdmetf5)
    }).collectAsMap()

    val csjjxxValues = sc.broadcast(value)
    val csjjxxValues2 =sc.broadcast(value2)
    val csjjxxValues3 =sc.broadcast(value3)

    val stream: DStream[String] = ssc.textFileStream("hdfs://192.168.102.120:8020/yss/guzhi/Test")
    stream.foreachRDD(rdd => {
      rdd.map(m => {
        val str = m.split(",")
        val zqdm = str(7) //证券代码
        val cjjg = str(10) //成交价格
        val bcrq=str(2) //本次日期
        val sql = "select 1 from csqyxx where fqylx='PG' " +
          "and FQYDJR<='" + bcrq + "' and fjkjzr>='" + bcrq +
          "' and FSTARTDATE<= '" + bcrq +
          "' and FQYBL not in('银行间','上交所','深交所','场外') " +
          "and FSH=1 and FZQDM='" + zqdm + "'"
        if (zqdm.startsWith("6")) {
          if (zqdm.startsWith("609")) return "CDRGP"
          else "GP"
        }
//        if (zqdm.startsWith("5")) {
//          if (cjjg.equals("0")) {
//            if (csjjxxValues.value.equals(zqdm)) return "EJDM"
//            if (csjjxxValues2.value.get(fzqdm)==(zqdm)) return "XJTD"
//            if (csjjxxValues3.value.get(fzqdm)==(zqdm)) return "XJTD_KSC"
//          }
//        } else return "JJ"
        if (zqdm.startsWith("0") || zqdm.startsWith("1")) return "ZQ"
        if (zqdm.startsWith("20")) return "HG"
        if (zqdm.startsWith("58")) return "QZ"
        if (zqdm.startsWith("712") || zqdm.startsWith("730") || zqdm.startsWith("731")
          || zqdm.startsWith("732") || zqdm.startsWith("780") || zqdm.startsWith("734")
          || zqdm.startsWith("740") || zqdm.startsWith("790")) return "XG"
        if (zqdm.startsWith("742")) return "QY"
        if (zqdm.startsWith("714") || zqdm.startsWith("760") || zqdm.startsWith("781")) {
          if (spark.sql(sql).count() > 0) return "QY"
          return "XG"
        }
        if (zqdm.startsWith("73")) return "XG"
        if (zqdm.startsWith("70")) {
          if ("100".equals(cjjg)) return "XZ"
          if (spark.sql(sql).count() > 0) return "QY"
          return "XG"
        }


      })

    })


    ssc.start()
    ssc.awaitTermination()

  }
}
