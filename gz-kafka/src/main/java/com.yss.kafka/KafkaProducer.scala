package com.yss.kafka

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import com.yss.kafka.Common._

object KafkaProducer {

  def getHDFSInputPath () : String = {
    val sdf = new SimpleDateFormat(inputPathPattern)
    val day = new Date()
    val date = sdf.format(day)
    "hdfs://bj-rack001-hadoop002:8020/yss/guzhi/basic_list/" + date + "/CSGDZH"
  }

  def getResult () : List[String] = {
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)
    val sqlcontext = new SQLContext(sc)
    val guDongRow = sc.textFile(getHDFSInputPath()).map(x=>{
      val guDongs = x.split(",")
      Row(
        guDongs(0).trim.toString,
        guDongs(1).trim.toString,
        guDongs(2).trim.toString,
        guDongs(3).trim.toString,
        guDongs(4).trim.toString,
        guDongs(5).trim.toString,
        guDongs(6).trim.toString,
        guDongs(7).trim.toString,
        guDongs(8).trim.toString
      )
    })
    val guDongSchema = StructType(Array(
      StructField("FGDDM",  StringType, true),
      StructField("FGDXM",    StringType, true),
      StructField("FSZSH",   StringType, true),
      StructField("FSH",   StringType, true),
      StructField("FZZR",   StringType, true),
      StructField("FSETCODE",  StringType, true),
      StructField("FCHK",StringType, true),
      StructField("FSTARTDATE",StringType, true),
      StructField("FACCOUNTTYPT",  StringType, true)))
    sqlcontext.createDataFrame(guDongRow,guDongSchema).registerTempTable(sqlTable)
    val guDongFrame = sqlcontext.sql(sqlText)
    val resultList = guDongFrame.rdd.map(row=>{
        row(0).toString + "," +
        row(1).toString + "," +
        row(2).toString + "," +
        row(3).toString + "," +
        row(4).toString + "," +
        row(5).toString + "," +
        row(6).toString + "," +
        row(7).toString + "," +
        row(8).toString + "," +
        row(9).toString
    }).collect().toList
    resultList
  }

  def main(args: Array[String]): Unit = {
    val prop = new Properties
    prop.put("zookeeper.connect" , zkConnect)
    prop.put("metadata.broker.list" , brokerList)
    prop.put("serializer.class" , serializableClass)
    val config = new ProducerConfig(prop)
    val producer = new Producer[String , String](config)
    val results = getResult()
    for(result <- results){
      val message = new KeyedMessage[String , String](kafkaTopic,result)
      producer.send(message)
    }
  }

}
