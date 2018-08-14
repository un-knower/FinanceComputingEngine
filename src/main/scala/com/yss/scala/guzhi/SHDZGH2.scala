package com.yss.scala.guzhi

import com.yss.scala.util.Util
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.math.BigDecimal.RoundingMode

case class DGH000012(FDATE: String,
                    FINDATE: String,
                    FZQDM: String,
                    FSZSH: String,
                    FJYXWH: String,
                    FBJE: String,
                    FSJE: String,
                    FBSL: String,
                    FSSL: String,
                    FBYJ: String,
                    FSYJ: String,
                    FBJSF: String,
                    FSJSF: String,
                    FBYHS: String,
                    FSYHS: String,
                    FBZGF: String,
                    FSZGF: String,
                    FBGHF: String,
                    FSGHF: String,
                    FBGZLX: String,
                    FSGZLX: String,
                    FHGGAIN: String,
                    FBFXJ: String,
                    FSFXJ: String,
                    FBSFJE: String,
                    FSSSJE: String,
                    FZQBZ: String,
                    FYWBZ: String,
                    FQSBZ: String,
                    FBQTF: String,
                    FSQTF: String,
                    ZQDM: String,
                    FJYFS: String,
                    FSH: String,
                    FZZR: String,
                    FCHK: String,
                    FZLH: String,
                    FTZBZ: String,
                    FBQSGHF: String,
                    FSQSGHF: String,
                    FGDDM: String)

/**
  * @author ws
  * @version 2018-08-08
  *          描述：上海大宗过户
  *          源文件：gdh.dbf
  *          结果表：HZJKQS
  */
object SHDZGH2 {

  def main(args: Array[String]): Unit = {
    doIt()
  }

  private def doIt(): Unit = {
    import com.yss.scala.dbf._

    val spark = SparkSession.builder().appName("SHDZGH").master("local[*]").getOrCreate()
    //    val df = spark.sqlContext.dbfFile("hdfs://nscluster/yss/guzhi/dgh00001.dbf")
    val df = spark.sqlContext.dbfFile("C:\\Users\\wuson\\Desktop\\new\\data\\dgh2250120180418.dbf")
    import spark.implicits._

    val value = df.rdd.map(row => {
      //TODO 判断文件是否正确
      if (row.size != 15) throw new Exception("文件内容有误,"+ row.size)
      val bcrq = row.getAs[String]("BCRQ") //日期/读入日期
      val zqdm = row.getAs[String]("ZQDM") //证券代码
      val gsdm = row.getAs[String]("GSDM") //交易席位
      val gddm = row.getAs[String]("GDDM") //股东代码
      val bs = row.getAs[String]("BS")
      val key = bcrq + "-" + zqdm + "-" + gsdm + "-" + gddm + "-" + bs
      (key, row)
    }).groupByKey().map {
      case (key, values) =>
        val fields = key.split("-")
        var FBje = BigDecimal(0)
        var FSje = BigDecimal(0)
        var FBsl = BigDecimal(0)
        var FSsl = BigDecimal(0)
        //默认费率
        val yjfl = BigDecimal(0.0026)
        val jsfl = BigDecimal(0.00012)
        val yhfl = BigDecimal(0.0011)
        val zgfl = BigDecimal(0.000041)
        val ghfl = BigDecimal(0.00051)
        val fxfl = BigDecimal(0.000036)
        //默认的折扣
        val jsfzk = BigDecimal(0.7)
        val qtzk = BigDecimal(1)

        for (row <- values) {
          val bs = row.getAs[String]("BS").trim
          val cjje = BigDecimal(row.getAs[String]("CJJE"))
          val cjsl = BigDecimal(row.getAs[String]("CJSL"))
          if ("B".equals(bs)) {
            FBje = FBje + cjje
            FBsl = FBsl + cjsl
          } else {
            FSje = FSje + cjje
            FSsl = FSsl + cjsl
          }
        }
        val FBjsf = FBje * jsfl * jsfzk
        val FBzgf = FBje * zgfl * qtzk
        val FBghf = FBsl * ghfl * qtzk
        val FBFxj = FBje * fxfl * qtzk
        val FByhs = BigDecimal(0)
        val FByj = FBje * yjfl * qtzk - FBzgf - FBjsf
        val FBsfje = FBje + FBjsf + FBzgf + FBghf

        val FSjsf = FSje * jsfl * jsfzk
        val FSzgf = FSje * zgfl * qtzk
        val FSghf = FSsl * ghfl * qtzk
        val FSFxj = FSje * fxfl * qtzk
        val FSyhs = FSje * yhfl * qtzk
        val FSyj = FSje * yjfl * qtzk - FSzgf - FSjsf
        val FSssje = FSje - FSjsf - FSzgf - FSghf - FSyhs

        val bcrq = fields(0)
        val FSzsh = "H"
        val Fjyxwh = fields(2)
        val FZqbz = "GP"
        val FYwbz = "DZ"
        val FQsbz = "N"
        val FBQtf = BigDecimal(0)
        val FSQtf = BigDecimal(0)
        val ZqDm = fields(1)
        val FJyFS = "PT"
        val Fsh = "1"
        val Fzzr = " "
        val Fchk = " "
        val fzlh = "0"
        val ftzbz = ""
        val FBQsghf = BigDecimal(0)
        val FSQsghf = BigDecimal(0)
        val FGddm = fields(3)
        val FHGGAIN = BigDecimal(0)
        val FSgzlx = BigDecimal(0)
        val FBgzlx = BigDecimal(0)

        DGH000012(bcrq, bcrq, ZqDm, FSzsh, Fjyxwh, FBje.setScale(2, RoundingMode.UP).formatted("%.2f"), FSje.setScale(2, RoundingMode.UP).formatted("%.2f"), FBsl.setScale(2, RoundingMode.UP).formatted("%.2f"), FSsl.setScale(2, RoundingMode.UP).formatted("%.2f"), FByj.setScale(2, RoundingMode.UP).formatted("%.2f"),
          FSyj.setScale(2, RoundingMode.UP).formatted("%.2f"), FBjsf.setScale(2, RoundingMode.UP).formatted("%.2f"), FSjsf.setScale(2, RoundingMode.UP).formatted("%.2f"), FByhs.setScale(2, RoundingMode.UP).formatted("%.2f"), FSyhs.setScale(2, RoundingMode.UP).formatted("%.2f"), FBzgf.setScale(2, RoundingMode.UP).formatted("%.2f"), FSzgf.setScale(2, RoundingMode.UP).formatted("%.2f"), FBghf.setScale(2, RoundingMode.UP).formatted("%.2f"), FSghf.setScale(2, RoundingMode.UP).formatted("%.2f"), FBgzlx.setScale(2, RoundingMode.UP).formatted("%.2f"),
          FSgzlx.setScale(2, RoundingMode.UP).formatted("%.2f"), FHGGAIN.setScale(2, RoundingMode.UP).formatted("%.2f"), FBFxj.setScale(2, RoundingMode.UP).formatted("%.2f"), FSFxj.setScale(2, RoundingMode.UP).formatted("%.2f"), FBsfje.setScale(2, RoundingMode.UP).formatted("%.2f"), FSssje.setScale(2, RoundingMode.UP).formatted("%.2f"), FZqbz, FYwbz, FQsbz, FBQtf.setScale(2, RoundingMode.UP).formatted("%.2f"), FSQtf.setScale(2, RoundingMode.UP).formatted("%.2f"),
          ZqDm, FJyFS, Fsh, Fzzr, Fchk, fzlh, ftzbz, FBQsghf.setScale(2, RoundingMode.UP).formatted("%.2f"), FSQsghf.setScale(2, RoundingMode.UP).formatted("%.2f"), FGddm)
    }
    Util.outputMySql(value.toDF(), "SHDZGH")
    spark.stop()
  }

}
