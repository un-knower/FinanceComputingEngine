package com.yss.guzhi.sparkbuss

import java.util.Properties

import com.yss.guzhi.sparkbuss.dto.QiHuoChengJiao
import com.yss.guzhi.sparkbuss.util.Util
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

/**
  * @author : 张海绥
  * @version : 2018-8-8
  *  describe: 期货成交明细
  *  目标文件：09000211trddata20180420.txt
  *  目标表：QHCJMX
  */
object QiHuoChengJiaoMingXi {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("QiHuoChengJiaoMingXi").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._

    //读取目标文件：期货成交明细
    val fileName = "09000211trddata20180420.txt"
    val readData: Dataset[String] = spark.read.textFile(Util.getInputFilePath(fileName))
    //val readData: Dataset[String] = spark.read.textFile("C:\\Users\\YZM\\Desktop\\test1.txt")

    //处理目标文件的每一行数据，得到最终结果
    val dsResult: Dataset[QiHuoChengJiao] = readData.map(line => {
      //将目标文件每一行数据切分成一个字符串数组
      val arr: Array[String] = line.split("@")
      //日期
      val fdate = arr(0)
      //客户内部资金账户
      val accountid = arr(1)
      //成交流水号
      val tradeid = arr(2)
      //品种合约
      val instrid = arr(3)
      //买卖标志
      val direction = arr(4)
      //成交量
      val tvolume = arr(5)
      //成交价
      val doubleTprice = arr(6).toDouble
      val tprice = f"$doubleTprice%.3f"
      //成交金额
      val doubleTamt = arr(7).toDouble
      val tamt = f"$doubleTamt%.3f"
      //成交时间
      val ttime = arr(8)
      //开平仓标志
      val offsetflag = arr(9)
      //投机套保标志
      val tzbz = arr(10)
      //平仓盈亏
      val doublePcyk_zr = arr(11).toDouble
      val pcyk_zr = f"$doublePcyk_zr%.3f"
      //平仓盈亏
      val doublePcyh_zb = arr(12).toDouble
      val pcyh_zb = f"$doublePcyh_zb%.3f"
      //手续费
      val transfee = arr(13)
      //交易编码
      val clientid = arr(14)
      //交易所统一标识
      val market = arr(15)
      //是否为非结算会员
      val hyflag = arr(16)
      //报单号
      val orderid = arr(17)
      //席位号
      val userid = arr(18)
      //监控中心编码
      val jshy = ""
      //文件名
      val partid = ""

      //结果返回一个对象，对象的属性为表格元数据信息
      QiHuoChengJiao(
        fdate,
        accountid,
        tradeid,
        instrid,
        direction,
        tvolume,
        tprice,
        tamt,
        ttime,
        offsetflag,
        tzbz,
        pcyk_zr,
        pcyh_zb,
        transfee,
        clientid,
        market,
        hyflag,
        orderid,
        userid,
        jshy,
        partid
      )
    })

    //将结果输出到Mysql数据库中
    val dfResult = dsResult.toDF()
    Util.outputMySql(dfResult,"QHCJMX")

  }
}
