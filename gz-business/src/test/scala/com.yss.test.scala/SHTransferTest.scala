package com.yss.test.scala

import com.yss.scala.guzhi.SHTransfer.{doETL, doExec, loadLvarlist}
import com.yss.scala.util.Util
import org.apache.spark.sql.SparkSession

/**
  * 上海过户测试类
  * @auther: wuson
  * @date: 2018/10/12
  * @version: 1.0.0
  * @desc:
  */
object SHTransferTest {

  def main(args: Array[String]): Unit = {
    testEtl(args(0))
  }

  /** 测试etl */
  def testEtl(fileName:String) = {
    val spark = SparkSession.builder().appName("SHDZGH").master("local[*]").getOrCreate()
    val broadcastLvarList = loadLvarlist(spark.sparkContext)
    //    loadTables(spark,"")
    val df = doETL(spark, broadcastLvarList,fileName)
    import spark.implicits._
    Util.outputMySql(df.toDF, "shgh_etl_test")
    spark.stop()
  }

  /** 测试Exec */
  def testExec(fileName:String,tableName:String) = {
    doExec(fileName,tableName)
  }


}
