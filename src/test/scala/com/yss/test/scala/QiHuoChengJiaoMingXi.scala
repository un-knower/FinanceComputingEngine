package com.yss.test.scala

import java.util.Properties
import com.yss.dto.QiHuoChengJiao
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

/**
  * @author : 张海绥
  * @version : 2018-8-8
  *  describe: 期货成交明细
  *  目标文件：09000211trddata20180420.txt
  *  目标表：09000211trddata20180420QiHuoChengJiaoMingXi
  */
object QiHuoChengJiaoMingXi {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("QiHuoChengJiaoMingXi").getOrCreate()
    import spark.implicits._
    val sc: SparkContext = spark.sparkContext
    //读取目标文件：期货成交明细
    val readData: Dataset[String] = spark.read.textFile("hdfs://nscluster/yss/guzhi/09000211trddata20180420.txt")
    //val readData: Dataset[String] = spark.read.textFile("C:\\Users\\YZM\\Desktop\\test1.txt")
    //处理目标文件的每一行数据，得到最终结果
    val Result: Dataset[QiHuoChengJiao] = readData.map(func = line => {
      //将目标文件每一行数据切分成一个字符串数组
      val arr: Array[String] = line.split("@")
      //成交日期
      val fdate = arr(20)
      //交易市场
      val market = arr(15)
      //品种
      val currency = arr(19)
      //交易代码
      val instrid = arr(3)
      //买卖
      val direction = arr(4)
      //成交价，保留三位小数
      val doubleTprice = arr(6).toDouble
      val tprice = f"$doubleTprice%.3f"
      //交易数量
      val tvolume = arr(5)
      //平开仓
      val offsetflag = arr(9)
      //交易类型,对应字段不清楚，暂时给0
      val trade = "0"
      //成交金额，保留三位小数
      val doubleTamt = arr(7).toDouble
      val tamt = f"$doubleTamt%.3f"
      //手续费，保留三位小数
      val doubleTransfee = arr(13).toDouble
      val transfee = f"$doubleTransfee%.3f"
      //交易类型，对应字段不清楚，暂时给0
      val trade2 = "0"
      //结算会员,默认“”
      val member = ""
      //佣金，max(成交金额*费率*折扣率，费用最低值）
      //费率目前假定为0.1，折扣率假定为0.8，费用最低值假定为100.0
      val doubleBrokerage = Math.max(doubleTamt*0.1*0.8,100.0)
      val brokerage = f"$doubleBrokerage%.3f"
      //结果返回一个对象，数据带有元数据信息
      QiHuoChengJiao(
        fdate,
        market,
        currency,
        instrid,
        direction,
        tprice,
        tvolume,
        offsetflag,
        trade,
        tamt,
        transfee,
        trade2,
        member,
        brokerage
      )
    })
    //将结果输出到Mysql数据库中
    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "root1234")
    Result.write.mode(SaveMode.Append).jdbc("jdbc:mysql://192.168.102.119/JJCWGZ", "09000211trddata20180420QiHuoChengJiaoMingXi", properties)

  }
}
