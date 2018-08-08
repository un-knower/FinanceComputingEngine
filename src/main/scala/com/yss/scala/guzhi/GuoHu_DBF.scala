package com.yss.scala.guzhi

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
  * @author 张锴
  * @version 2018-08-08
  *  描述：上海过户表
  *   源文件：gh.dbf
  *   结果表：HZJKQS
  */
object GuoHu_DBF {

  case class HZJKQS(FDATE: String,
                    FINDATE: String,
                    FZQDM: String,
                    FSZSH: String,
                    FJYXWH: String,
                    FBJE: BigDecimal,
                    FSJE: BigDecimal,
                    FBSL: BigDecimal,
                    FSSL: BigDecimal,
                    FBYJ: BigDecimal,
                    FSYJ: BigDecimal,
                    FBJSF: BigDecimal,
                    FSJSF: BigDecimal,
                    FBYHS: BigDecimal,
                    FSYHS: BigDecimal,
                    FBZGF: BigDecimal,
                    FSZGF: BigDecimal,
                    FBGHF: BigDecimal,
                    FSGHF: BigDecimal,
                    FBGZLX: BigDecimal,
                    FSGZLX: BigDecimal,
                    FBFXJ: BigDecimal,
                    FSFXJ: BigDecimal,
                    FBSFJE: BigDecimal,
                    FSSSJE: BigDecimal,
                    FHGGAIN: BigDecimal,
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
                    FBQSGHF: BigDecimal,
                    FSQSGHF: BigDecimal,
                    FGDDM: String
                   )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("mytest")
      .master("local[2]")
      .getOrCreate()
    import com.yss.scala.dbf._
    val value = spark.sqlContext.dbfFile("E:\\DBFFILE\\InterDoc\\gh.dbf").rdd.map(x => {
      val Fdate = x.getAs[String]("BCRQ")
      //日期
      val FinDate = x.getAs[String]("BCRQ")
      //读入日期
      val FZqdm = x.getAs[String]("ZQDM")
      //证券代码
      val Fjyxwh = x.getAs[String]("GSDM")
      //交易席位号
      val ZQDM = x.getAs[String]("ZQDM")
      //证券代码
      val fgddm = x.getAs[String]("GDDM")
      //股东代码
      (Fdate + "_" + FinDate + "_" + FZqdm + "_" + Fjyxwh + "_" + ZQDM + "_" + fgddm, x)
    }).groupByKey()

    val v: RDD[HZJKQS] = value.map(t => {
      val fs = t._1.split("_")
      val Fdate = fs(0)
      val FinDate = fs(1)
      val FZqdm = fs(2)
      val Fjyxwh = fs(3)
      val ZQDM = fs(4)
      val fgddm = fs(5)

      var Fbje = BigDecimal(0.0)
      var Fsje = BigDecimal(0.0)
      var FBsl = BigDecimal(0.0)
      var FSsl = BigDecimal(0.0)
      var Fbyj = BigDecimal(0.0)
      var Fsyj = BigDecimal(0.0)
      var FBjsf = BigDecimal(0.0)
      var FSjsf = BigDecimal(0.0)
      var Fbyhs = BigDecimal(0.0)
      var Fsyhs = BigDecimal(0.0)
      var FBzgf = BigDecimal(0.0)
      var FSzgf = BigDecimal(0.0)
      var FBghf = BigDecimal(0.0)
      var FSghf = BigDecimal(0.0)
      val FBgzlx = BigDecimal(0)
      val FSgzlx = BigDecimal(0)
      var FBfxj = BigDecimal(0.0)
      var FSfxj = BigDecimal(0.0)
      var Fbsfje = BigDecimal(0.0)
      var Fsssje = BigDecimal(0.0)
      var FHggain = BigDecimal(0.0)
      val FSzsh = "H"
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
      val FBQsghf = BigDecimal(0.0)
      val FsQsghf = BigDecimal(0.0)
      //默认费率
      val yongjin = 0.0025
      val yinghua = 0.001
      val guohu = 0.0005
      val jingshou = 0.00011
      val zhengguan = 0.00004
      val fengxianjin = 0.00003

      val fileds = t._2.mkString(",").split(",")
      val bs = fileds(13)
      var cjje = BigDecimal(fileds(11))
      var cjsl = BigDecimal(fileds(5))
      for (x <- t._2) {
        if ("B".equals(bs)) {
          Fbje = cjje
          FBsl = cjsl
          Fbyj = Fbje * yongjin - zhengguan - guohu - yinghua
          FBjsf = Fbje * jingshou
          Fbyhs = Fbje * yinghua
          FBzgf = Fbje * zhengguan
          FBghf = Fbje * guohu
          FBfxj = Fbje * fengxianjin
          Fbsfje = Fbje + FBjsf + FBzgf + FBghf
        } else {
          Fsje = cjje
          FSsl = cjsl
          Fsyj = Fbje * yongjin - zhengguan - guohu - yinghua
          FSjsf = Fbje * jingshou
          Fsyhs = Fbje * yinghua
          FSzgf = Fbje * zhengguan
          FSghf = Fbje * guohu
          FSfxj = Fbje * fengxianjin
          Fsssje = Fsje - FSjsf - FSzgf - FSghf - Fsyhs
        }
      }
      HZJKQS(Fdate, FinDate, FZqdm, FSzsh, Fjyxwh, Fbje, Fsje, FBsl, FSsl, Fbyj, Fsyj, FBjsf, FSjsf, Fbyhs, Fsyhs, FBzgf, FSzgf, FBghf
        , FSghf, FBgzlx, FSgzlx, FBfxj, FSfxj, Fbsfje, Fsssje, FHggain, FZqbz, Fywbz, FQsbz, FBQTF, FSQTF, ZQDM, FJYFS, Fsh, FZZR, FCHK, fzlh, ftzbz
        , FBQsghf, FsQsghf, fgddm)
    })
    import spark.implicits._
    v.toDF()
      .write.format("jdbc")
      .option("url", "jdbc:mysql://192.168.102.119:3306/JJCWGZ?useUnicode=true&characterEncoding=utf8")
      .option("dbtable", "HZJKQS")
      .option("user", "root")
      .option("password", "root1234")
      .mode(SaveMode.Append)
      .save()
  }
}
