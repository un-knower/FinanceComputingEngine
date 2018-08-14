package com.yss.scala.guzhi

import com.yss.scala.dto.ShangHaiGuoHu
import com.yss.scala.util.Util
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import scala.math.BigDecimal.RoundingMode

/**
  * @author : 张海绥
  * @version : 2018-8-13
  *  describe: 上海过户管理
  *  目标表：SHGH
  */
object SHGH {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("mytest")
      .master("local[2]")
      .getOrCreate()

    import com.yss.scala.dbf._
    import spark.implicits._

    //读取上海过户目标文件，并将数据注册成一张临时表
    val dbfFileFrame: DataFrame = spark.sqlContext.dbfFile("C:\\Users\\YZM\\Desktop\\gh23341.dbf")
    dbfFileFrame.createOrReplaceTempView("dbfFileTable")

    //对上海过户文件进行sum汇总
    val sumDbfFileFrame: DataFrame = spark.sql("select GDDM,BCRQ,GSDM,ZQDM,BS,sum(CAST(CJSL AS decimal(15,2))) as CJSL,sum(CAST(CJJE AS decimal(15,2))) as CJJE from dbfFileTable group by GDDM,BCRQ,GSDM,ZQDM,BS")

    //获取结果表中各个字段数据
    val dsResult: Dataset[ShangHaiGuoHu] = sumDbfFileFrame.map(line => {

      //上海过户默认费率
      //佣金费率
      val rateYJ = BigDecimal(0.0025)
      //经手费率
      val rateJS = BigDecimal(0.00011)
      //折扣率
      val zk = BigDecimal(1.0)
      //印花费率
      val rateYH = BigDecimal(0.001)
      //征管费率
      val rateZG = BigDecimal(0.00004)
      //过户费率
      val rateGH = BigDecimal(0.0005)
      //风险金费率
      val rateFXJ = BigDecimal(0.00003)

      //买卖方向
      val BS = line.getAs[String]("BS")
      //日期
      val FDATE: String = line.getAs[String]("BCRQ")
      //读入日期
      val FINDATE = line.getAs[String]("BCRQ")
      //证券代码
      val FZQDM = line.getAs[String]("ZQDM")
      //市场
      val FSZSH = "H"
      //交易席位号
      val FJYXWH = line.getAs[String]("GSDM")
      //买金额
      var FBJE: BigDecimal = BigDecimal(0.0).setScale(2, RoundingMode.HALF_UP)
      if (BS == "B") FBJE = BigDecimal(String.valueOf(line.get(6))).setScale(2, RoundingMode.HALF_UP)
      //卖金额
      var FSJE: BigDecimal = BigDecimal(0.0).setScale(2, RoundingMode.HALF_UP)
      if (BS == "S") FSJE = BigDecimal(String.valueOf(line.get(6))).setScale(2, RoundingMode.HALF_UP)
      //买数量
      var FBSL: BigDecimal = BigDecimal(0.0).setScale(2, RoundingMode.HALF_UP)
      if (BS == "B") FSJE = BigDecimal(String.valueOf(line.get(5))).setScale(2, RoundingMode.HALF_UP)
      //卖数量
      var FSSL: BigDecimal = BigDecimal(0.0).setScale(2, RoundingMode.HALF_UP)
      if (BS == "S") FSJE = BigDecimal(String.valueOf(line.get(5))).setScale(2, RoundingMode.HALF_UP)
      //买经手费
      var FBJSF: BigDecimal = BigDecimal(0.0).setScale(2, RoundingMode.HALF_UP)
      if (BS == "B") FBJSF = (FBJE * rateJS).setScale(2, RoundingMode.HALF_UP)
      //卖经手费
      var FSJSF: BigDecimal = BigDecimal(0.0).setScale(2, RoundingMode.HALF_UP)
      if (BS == "S") FSJSF = (FSJE * rateJS).setScale(2, RoundingMode.HALF_UP)
      //买印花税
      var FBYHS: BigDecimal = BigDecimal(0.0).setScale(2, RoundingMode.HALF_UP)
      //卖印花税
      var FSYHS: BigDecimal = BigDecimal(0.0).setScale(2, RoundingMode.HALF_UP)
      if (BS == "S") FSYHS = (FSJE * rateYH).setScale(2, RoundingMode.HALF_UP)
      //买征管费
      var FBZGF: BigDecimal = BigDecimal(0.0).setScale(2, RoundingMode.HALF_UP)
      if (BS == "B") FBZGF = (FBJE * rateZG).setScale(2, RoundingMode.HALF_UP)
      //卖征管费
      var FSZGF: BigDecimal = BigDecimal(0.0).setScale(2, RoundingMode.HALF_UP)
      if (BS == "S") FSZGF = (FSJE * rateZG).setScale(2, RoundingMode.HALF_UP)
      //买过户费
      var FBGHF: BigDecimal = BigDecimal(0.0).setScale(2, RoundingMode.HALF_UP)
      if (BS == "B") FBGHF = (FBJE * rateGH).setScale(2, RoundingMode.HALF_UP)
      //卖过户费
      var FSGHF: BigDecimal = BigDecimal(0.0).setScale(2, RoundingMode.HALF_UP)
      if (BS == "S") FSGHF = (FSJE * rateGH).setScale(2, RoundingMode.HALF_UP)
      //买佣金
      var FBYJ: BigDecimal = BigDecimal(0.0).setScale(2, RoundingMode.HALF_UP)
      if (BS == "B") FBYJ = (FBJE * rateYJ - FBZGF - FBGHF-FBYHS ).setScale(2, RoundingMode.HALF_UP)
      //卖佣金
      var FSYJ: BigDecimal = BigDecimal(0.0).setScale(2, RoundingMode.HALF_UP)
      if (BS == "S") FSYJ = (FSJE * rateYJ - FSZGF - FSGHF - FSYHS).setScale(2, RoundingMode.HALF_UP)
      //买国债利息,默认值0.0
      val FBGZLX: BigDecimal = BigDecimal(0.0).setScale(2, RoundingMode.HALF_UP)
      //卖国债利息,默认值0.0
      val FSGZLX: BigDecimal = BigDecimal(0.0).setScale(2, RoundingMode.HALF_UP)
      //买风险金
      var FBFXJ: BigDecimal = BigDecimal(0.0).setScale(2, RoundingMode.HALF_UP)
      if (BS == "B") FBFXJ = (FBJE * rateFXJ).setScale(2, RoundingMode.HALF_UP)
      //卖风险金
      var FSFXJ: BigDecimal = BigDecimal(0.0).setScale(2, RoundingMode.HALF_UP)
      if (BS == "S") FSFXJ = (FSJE * rateFXJ).setScale(2, RoundingMode.HALF_UP)
      //买实付金额
      val FBSFJE: BigDecimal = (FBJE + FBJSF + FBZGF + FBGHF ).setScale(2, RoundingMode.HALF_UP)
      //卖实收金额
      val FSSSJE: BigDecimal = (FSJE - FSJSF - FSZGF - FSGHF - FSYHS).setScale(2, RoundingMode.HALF_UP)
      //证券标志，目前只处理GP
      val FZQBZ: String = "GP"
      //业务标志，PT，目前只处理PT
      val FYWBZ: String = "PT"
      //清算标志N
      val FQSBZ: String = "N"
      //买其他费用，默认值0
      val FBQTF: String = "0"
      //卖其他费用，默认值0
      val FSQTF: String = "0"
      //证券代码
      val ZQDM: String = line.getAs[String]("ZQDM")
      //交易方式
      val FJYFS: String = "PT"
      //审核
      val FSH: String = "1"
      //制作人
      val FZZR: String = ""
      //审核人
      val FCHK: String = ""
      //指令号
      val FZLH: String = "0"
      //投资标志
      val FTZBZ: String = ""
      //买券商过户费
      val FBQSGHF: BigDecimal = BigDecimal(0.0).setScale(2, RoundingMode.HALF_UP)
      //卖券商过户费
      val FSQSGHF: BigDecimal = BigDecimal(0.0).setScale(2, RoundingMode.HALF_UP)
      //股东代码
      val FGDDM: String = line.getAs[String]("GDDM")
      //回购收益,给一个默认值
      val FHGGAIN: BigDecimal = BigDecimal(0.0).setScale(2, RoundingMode.HALF_UP)

      //返回一个对象，对象属性为表元数据信息
      ShangHaiGuoHu(
        FDATE,
        FINDATE,
        FZQDM,
        FSZSH,
        FJYXWH,
        FBJE.formatted("%.2f"),
        FSJE.formatted("%.2f"),
        FBSL.formatted("%.2f"),
        FSSL.formatted("%.2f"),
        FBYJ.formatted("%.2f"),
        FSYJ.formatted("%.2f"),
        FBJSF.formatted("%.2f"),
        FSJSF.formatted("%.2f"),
        FBYHS.formatted("%.2f"),
        FSYHS.formatted("%.2f"),
        FBZGF.formatted("%.2f"),
        FSZGF.formatted("%.2f"),
        FBGHF.formatted("%.2f"),
        FSGHF.formatted("%.2f"),
        FBGZLX.formatted("%.2f"),
        FSGZLX.formatted("%.2f"),
        FBFXJ.formatted("%.2f"),
        FSFXJ.formatted("%.2f"),
        FBSFJE.formatted("%.2f"),
        FSSSJE.formatted("%.2f"),
        FZQBZ,
        FYWBZ,
        FQSBZ,
        FBQTF,
        FSQTF,
        ZQDM,
        FJYFS,
        FSH,
        FZZR,
        FCHK,
        FZLH,
        FTZBZ,
        FBQSGHF.formatted("%.2f"),
        FSQSGHF.formatted("%.2f"),
        FGDDM,
        FHGGAIN.formatted("%.2f")
      )

    })
    //将结果输出到MySql中
    val dfResult = dsResult.toDF()
    Util.outputMySql(dfResult,"SHGH")

  }
}
