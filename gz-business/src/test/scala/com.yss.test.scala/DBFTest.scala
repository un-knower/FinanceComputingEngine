package com.yss.test.scala

import com.yss.scala.util.Util
import org.apache.spark.sql.SparkSession

/**
  * 读取dbf文件
  */
object DBFTest {

  def main(args: Array[String]): Unit = {
    //注意引入隐式转换
    import com.yss.scala.dbf.dbf._
    val spark = SparkSession.builder().appName("mytest").master("local[*]").getOrCreate()
    val df = spark.sqlContext.dbfFile("C:\\Users\\wuson\\Desktop\\GuZhi\\createdbf4.dbf")
    df.rdd.saveAsTextFile("C:\\Users\\wuson\\Desktop\\GuZhi\\tt")

    spark.stop()

    //    sess.sparkContext.hadoopFile("C:\\Users\\wuson\\Desktop\\new\\reff040704.txt", classOf[TextInputFormat], classOf[LongWritable], classOf[Text])
    //      .map(pair => new String(pair._2.getBytes, 0, pair._2.getLength, "GBK")).foreach(println(_))
    //      .saveAsTextFile("C:\\Users\\wuson\\Desktop\\new\\reff040705.txt")

  }
}
