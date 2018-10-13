package com.yss.scala.guzhi


import com.yss.scala.util.Util
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.math.BigDecimal.RoundingMode

/**
  * @author 张锴
  * @version 2018-08-08
  *          描述：上海过户表
  *          源文件：gh.dbf
  *          结果表：HZJKQS
  */
object GuoHu {


  case class HZJKQS(
                     FDATE: String,
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
                     FBFXJ: String,
                     FSFXJ: String,
                     FBSFJE: String,
                     FSSSJE: String,
                     FHGGAIN: String,
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
                     FGDDM: String
                   )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("mytest")
      .master("local[2]")
      .getOrCreate()
    import com.yss.scala.dbf.dbf._

    //hdfs://nscluster/yss/guzhi/gh.dbf
    val value = spark.sqlContext.dbfFile("E:\\DBFFILE\\InterDoc\\gh23341.dbf").rdd.map(row => {

      val Fdate = row.getAs[String]("BCRQ") //日期

      val FinDate = row.getAs[String]("BCRQ") //读入日期

      val FZqdm = row.getAs[String]("ZQDM") //证券代码

      val Fjyxwh = row.getAs[String]("GSDM") //交易席位号

      //val fgddm = row.getAs[String]("GDDM") //股东代码
      val bs = row.getAs[String]("BS") //买卖

      (Fdate + "_" + FinDate + "_" + FZqdm + "_" + Fjyxwh + "_" + bs, row)
    }).groupByKey().map(t => {
      val fs = t._1.split("_")
      val Fdate = fs(0)
      val FinDate = fs(1)
      val FZqdm = fs(2)
      val Fjyxwh = fs(3)
      // val fgddm = fs(4)
      val bs = fs(4)

      var Fbje = BigDecimal(0)
      var Fsje = BigDecimal(0)
      var FBsl = BigDecimal(0)
      var FSsl = BigDecimal(0)
      var Fbyj = BigDecimal(0)
      var Fsyj = BigDecimal(0)
      var FBjsf = BigDecimal(0)
      var FSjsf = BigDecimal(0)
      var Fbyhs = BigDecimal(0)
      var Fsyhs = BigDecimal(0)
      var FBzgf = BigDecimal(0)
      var FSzgf = BigDecimal(0)
      var FBghf = BigDecimal(0)
      var FSghf = BigDecimal(0)
      val FBgzlx = "0"
      val FSgzlx = "0"
      var FBfxj = BigDecimal(0)
      var FSfxj = BigDecimal(0)
      var FHggain = "0"

      val FSzsh = "H" //市场
      val FZqbz = "GP"
      val Fywbz = "PT"
      val FQsbz = "N"
      val FBQTF = "0"
      val FSQTF = "0"
      val FJYFS = "PT"
      val Fsh = " "
      val FZZR = " "
      val FCHK = " "
      val fzlh = "0"
      val ftzbz = " "
      val FBQsghf = "0"
      val FSQsghf = "0"

      //默认费率
      val rateYJ = BigDecimal(0.0025)
      val rateJS = BigDecimal(0.00011)
      val zk = BigDecimal(1)
      val rateYH = BigDecimal(0.001)
      val rateZG = BigDecimal(0.000041)
      val rateGH = BigDecimal(0.00004)
      val rateFXJ = BigDecimal(0.00003)

      var sumCjje = BigDecimal(0)
      var sumCjsl = BigDecimal(0)
      var sumYj = BigDecimal(0)
      var sumJsf = BigDecimal(0)
      var sumYhs = BigDecimal(0)
      var sumZgf = BigDecimal(0)
      var sumGhf = BigDecimal(0)
      var sumFxj = BigDecimal(0)

      val fileds = t._2.mkString(",").split(",")
      //val Fjyxwh = fileds(4)
      val ZQDM = fileds(7)
      val fgddm = fileds(0)

      for (x <- t._2) {
        val cjje = BigDecimal(x.getAs[String]("CJJE"))
        //成交金额
        val cjsl = BigDecimal(x.getAs[String]("CJSL")) //成交数量

        //        val jsf = cjje * rateJS * zk.setScale(2, RoundingMode.HALF_UP)
        //        val yhs = cjje * rateYH * zk.setScale(2, RoundingMode.HALF_UP)
        //        val zgf = cjje * rateZG * zk.setScale(2, RoundingMode.HALF_UP)
        //        val ghf = cjje * rateGH * zk.setScale(2, RoundingMode.HALF_UP)
        //        val fx = cjje * rateFXJ * zk.setScale(2, RoundingMode.HALF_UP)
        //        val yj = cjje * rateYJ.setScale(2, RoundingMode.HALF_UP) - zgf - jsf
        //        sumCjje = (sumCjje + cjje).setScale(2, RoundingMode.HALF_UP)
        //        sumCjsl = (sumCjsl + cjsl).setScale(2, RoundingMode.HALF_UP)
        //        sumYj = sumYj + yj
        //        sumJsf = sumJsf + jsf
        //        sumYhs = sumYhs + yhs
        //        sumZgf = sumZgf + zgf
        //        sumGhf = sumGhf + ghf
        //        sumFxj = sumFxj + fx
        //      }
        //      if ("B".equals(bs)) {
        //        Fbje = sumCjje
        //        FBsl = sumCjsl
        //        FBjsf = sumJsf
        //        Fbyhs = BigDecimal(0)
        //        FBzgf = sumZgf
        //        FBghf = sumGhf
        //        FBfxj = sumFxj
        //        Fbyj = sumYj
        //      } else {
        //        Fsje = sumCjje
        //        FSsl = sumCjsl
        //        FSjsf = sumJsf
        //        Fsyhs = sumYhs
        //        FSzgf = sumZgf
        //        FSghf = sumGhf
        //        FSfxj = sumFxj
        //        Fsyj = sumYj
        //      }
        val jsf = (cjje * rateJS * zk).setScale(2, RoundingMode.HALF_UP)
        var yhs = BigDecimal(0)
        if ("S".equals(bs)) {
          yhs = (cjje * rateYH * zk).setScale(2, RoundingMode.HALF_UP)
        }
        val zgf = (cjje * rateZG * zk).setScale(2, RoundingMode.HALF_UP)
        val ghf = (cjsl * rateGH * zk).setScale(2, RoundingMode.HALF_UP)
        val fx = (cjje * rateFXJ * zk).setScale(2, RoundingMode.HALF_UP)
        val yj = (cjje * rateYJ * zk).setScale(2, RoundingMode.HALF_UP) - zgf - jsf
        sumCjje = sumCjje + cjje
        sumCjsl = sumCjsl + cjsl
        sumYj = sumYj + yj
        sumJsf = sumJsf + jsf
        sumYhs = sumYhs + yhs
        sumZgf = sumZgf + zgf
        sumGhf = sumGhf + ghf
        sumFxj = sumFxj + fx
      }

      if ("B".equals(bs)) {
        Fbje = sumCjje
        FBsl = sumCjsl
        FBjsf = sumJsf
        FBzgf = sumZgf
        FBghf = sumGhf
        FBfxj = sumFxj
        Fbyj = sumYj
      } else {
        Fsje = sumCjje
        FSsl = sumCjsl
        FSjsf = sumJsf
        Fsyhs = sumYhs
        FSzgf = sumZgf
        FSghf = sumGhf
        FSfxj = sumFxj
        Fsyj = sumYj
      }
      val Fbsfje = (Fbje + FBjsf + FBzgf + FBghf).setScale(2, RoundingMode.HALF_UP)
      val Fsssje = (Fsje - FSjsf - FSzgf - FSghf - Fsyhs).setScale(2, RoundingMode.HALF_UP)

      HZJKQS(
        Fdate,
        FinDate,
        FZqdm,
        FSzsh,
        Fjyxwh,
        Fbje.formatted("%.2f"),
        Fsje.formatted("%.2f"),
        FBsl.formatted("%.2f"),
        FSsl.formatted("%.2f"),
        Fbyj.formatted("%.2f"),
        Fsyj.formatted("%.2f"),
        FBjsf.formatted("%.2f"),
        FSjsf.formatted("%.2f"),
        Fbyhs.formatted("%.2f"),
        Fsyhs.formatted("%.2f"),
        FBzgf.formatted("%.2f"),
        FSzgf.formatted("%.2f"),
        FBghf.formatted("%.2f"),
        FSghf.formatted("%.2f"),
        FBgzlx,
        FSgzlx,
        FBfxj.formatted("%.2f"),
        FSfxj.formatted("%.2f"),
        Fbsfje.formatted("%.2f"),
        Fsssje.formatted("%.2f"),
        FHggain,
        FZqbz,
        Fywbz,
        FQsbz,
        FBQTF,
        FSQTF,
        ZQDM,
        FJYFS,
        Fsh,
        FZZR,
        FCHK,
        fzlh,
        ftzbz,
        FBQsghf,
        FSQsghf,
        fgddm
      )
    })
    import spark.implicits._
    Util.outputMySql(value.toDF(), "HZJKQS")
    spark.stop()
  }
}
