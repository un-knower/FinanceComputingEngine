package com.yss.kafka

import com.yss.spark.KafkaUtilsSpark
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author wangshuai
  * @version 2018-09-27 09:31
  *          describe: 
  *          目标文件：
  *          目标表：
  */
object SparkStreaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark接受kafka数据").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(1))
    val rdd = KafkaUtilsSpark.getStream(ssc)
    rdd.filter(x => x.currentRecord ==1 ).
      foreachRDD(x => x.foreach(a => println(s"${a.fileName}|${a.rowValue}|${a.currentRecord}")))
    ssc.start()
    ssc.awaitTermination()
  }
}
