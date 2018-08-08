package com.yss.scala.guzhi

import java.sql.{Connection, PreparedStatement}
import java.util.Date

import com.yss.scala.util.{MysqlUtil, XMLReader}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * 将imcexchangerate.xml的数据写入mysql数据库中
  *
  * @author zhangyl
  * @date 018/8/7
  *
  */
object ImcExchangeRateToMysql {
  def main(args: Array[String]): Unit = {

    val sqlContext = SparkSession.builder().master("local[*]").getOrCreate()

    val inputPath = "F:/work/evaluation/test_data/imcexchangerate.xml"

    //数据读取及处理
    XMLReader.readXML(inputPath, sqlContext).foreachPartition(insertDataToMysqlFun)

    //按照分区将数据写入MySQL数据库
    def insertDataToMysqlFun(itr: Iterator[Row]): Unit = {
      var conn: Connection = null
      var ps: PreparedStatement = null
      val sql = "insert into imcexchangerate" +
        "(sjlx, mrhl, mchl, zjhl, bz, rq, BYs) " +
        "values " +
        "('102', ?, ?, ?, ?, ?, ' ')"
      try {
        //conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/guzhi", "root", "123456")
        conn = MysqlUtil.getConn()
        val now = new Date().getTime

        itr.foreach(row => {
          ps = conn.prepareStatement(sql)
          ps.setDouble(1, row(0).toString.toDouble)
          ps.setDouble(2, row(3).toString.toDouble)
          ps.setDouble(3, row(2).toString.toDouble)
          ps.setString(4, row(1).toString)
          ps.setLong(5,now)
          ps.executeUpdate()
        })
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        if (ps != null) {
          ps.close()
        }
        if (conn != null) {
          conn.close()
        }
      }
    }

    sqlContext.stop()

  }
}
