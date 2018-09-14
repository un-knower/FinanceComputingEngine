package com.yss.scala.guzhi

import com.yss.scala.dbf.dbf._
import com.yss.scala.util.DateUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

/**
  * @auther: lijiayan
  * @aate: 2018/9/13
  * @desc: 上海固定收益三方回购
  */
object GuDingShouYiSFHGETL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(GDSYSFHG.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    val jsmxRDD: RDD[Row] = readJsmxFileAndFilted(spark, args(0))

    val jsmx013FiltedRDD = jsmxRDD.filter(row => {
      val YWLX = row.getAs[String]("YWLX")
      "680".equals(YWLX) || "681".equals(YWLX) || "683".equals(YWLX)
    })

    val jsmx013SFHGETL: RDD[SFHGETL] = jsmx013Entity(jsmx013FiltedRDD)

    val jsmx2RDD: RDD[Row] = jsmxRDD.subtract(jsmx013FiltedRDD)
    val wdqRDD: RDD[Row] = readWdqFileAndFilted(spark, args(1))

    val jsmx2SFHGETL: RDD[SFHGETL] = jsmx2Entity(jsmx2RDD, wdqRDD)

    val res = jsmx013SFHGETL.union(jsmx2SFHGETL)

    saveAsCSV(spark, res, "C:\\Users\\yss\\Desktop\\savetemp\\" + System.currentTimeMillis())

    spark.stop()
  }


  private def saveAsCSV(spark: SparkSession, res: RDD[SFHGETL], path: String): Unit = {
    import spark.implicits._
    res.repartition(1).toDF(
      "SCDM",
      "JLLX",
      "JYFS",
      "JSFS",
      "YWLX",
      "QSBZ",
      "GHLX",
      "JSBH",
      "CJBH",
      "SQBH",
      "WTBH",
      "JYRQ",
      "QSRQ",
      "JSRQ",
      "QTRQ",
      "WTSJ",
      "CJSJ",
      "XWH1",
      "XWH2",
      "XWHY",
      "JSHY",
      "TGHY",
      "ZQZH",
      "ZQDM1",
      "ZQDM2",
      "ZQLB",
      "LTLX",
      "QYLB",
      "GPNF",
      "MMBZ",
      "SL",
      "CJSL",
      "ZJZH",
      "BZ",
      "JG1",
      "JG2",
      "QSJE",
      "YHS",
      "JSF",
      "GHF",
      "ZGF",
      "SXF",
      "QTJE1",
      "QTJE2",
      "QTJE3",
      "SJSF",
      "JGDM",
      "FJSM",
      "ZQBS",
      "JYLB",
      "QTRQCJRQ"
    ).write.option("header", value = true).csv(path)
  }

  private def jsmx2Entity(jsmx2RDD: RDD[Row], wdqRDD: RDD[Row]) = {

    val keyJSMXRDD: RDD[(String, Row)] = jsmx2RDD.map(row => {
      val CJBH = row.getAs[String]("CJBH").trim
      val ZQZH = row.getAs[String]("ZQZH").trim
      val ZQDM1 = row.getAs[String]("ZQDM1").trim
      val XWH1 = row.getAs[String]("XWH1").trim
      (CJBH + ZQZH + ZQDM1 + XWH1, row)
    })

    val keyWDQRDD = wdqRDD.map(row => {
      val CJXLH = row.getAs[String]("CJXLH").trim
      val ZQZH = row.getAs[String]("ZQZH").trim
      val ZQDM = row.getAs[String]("ZQDM").trim
      val XWH1 = row.getAs[String]("XWH1").trim
      (CJXLH + ZQZH + ZQDM + XWH1, row)
    })

    val jsmxAndWdqRDD: RDD[(String, (Row, Row))] = keyJSMXRDD.join(keyWDQRDD)
    jsmxAndWdqRDD.persist()

    val xzljRDD = jsmxAndWdqRDD.map(row => {
      val jsmxRow = row._2._1
      val wdqRow = row._2._2
      val SCDM = jsmxRow.getAs[String]("SCDM")
      val JLLX = jsmxRow.getAs[String]("JLLX")
      val JYFS = jsmxRow.getAs[String]("JYFS")
      val JSFS = jsmxRow.getAs[String]("JSFS")
      val YWLX = jsmxRow.getAs[String]("YWLX")
      val QSBZ = jsmxRow.getAs[String]("QSBZ")
      val GHLX = jsmxRow.getAs[String]("GHLX")
      val JSBH = jsmxRow.getAs[String]("JSBH")
      val CJBH = jsmxRow.getAs[String]("CJBH")
      val SQBH = jsmxRow.getAs[String]("SQBH")
      val WTBH = jsmxRow.getAs[String]("WTBH")
      val JYRQ = jsmxRow.getAs[String]("JYRQ")
      val QSRQ = jsmxRow.getAs[String]("QSRQ")
      val JSRQ = jsmxRow.getAs[String]("JSRQ")
      val QTRQ = jsmxRow.getAs[String]("QTRQ")
      val WTSJ = jsmxRow.getAs[String]("WTSJ")
      val CJSJ = jsmxRow.getAs[String]("CJSJ")
      val XWH1 = jsmxRow.getAs[String]("XWH1")
      val XWH2 = jsmxRow.getAs[String]("XWH2")
      val XWHY = jsmxRow.getAs[String]("XWHY")
      val JSHY = jsmxRow.getAs[String]("JSHY")
      val TGHY = jsmxRow.getAs[String]("TGHY")
      val ZQZH = jsmxRow.getAs[String]("ZQZH")
      val ZQDM1 = jsmxRow.getAs[String]("ZQDM1")
      val ZQDM2 = jsmxRow.getAs[String]("ZQDM2")
      val ZQLB = jsmxRow.getAs[String]("ZQLB")
      val LTLX = jsmxRow.getAs[String]("LTLX")
      val QYLB = jsmxRow.getAs[String]("QYLB")
      val GPNF = jsmxRow.getAs[String]("GPNF")
      val MMBZ = jsmxRow.getAs[String]("MMBZ")
      val SL = jsmxRow.getAs[String]("SL")
      val CJSL = jsmxRow.getAs[String]("CJSL")
      val ZJZH = jsmxRow.getAs[String]("ZJZH")
      val BZ = jsmxRow.getAs[String]("BZ")
      val JG1 = jsmxRow.getAs[String]("JG1")
      val JG2 = jsmxRow.getAs[String]("JG2")
      val QSJE = jsmxRow.getAs[String]("QSJE")
      val YHS = jsmxRow.getAs[String]("YHS")
      val JSF = jsmxRow.getAs[String]("JSF")
      val GHF = jsmxRow.getAs[String]("GHF")
      val ZGF = jsmxRow.getAs[String]("ZGF")
      val SXF = jsmxRow.getAs[String]("SXF")
      val QTJE1 = jsmxRow.getAs[String]("QTJE1")
      val QTJE2 = jsmxRow.getAs[String]("QTJE2")
      val QTJE3 = jsmxRow.getAs[String]("QTJE3")
      val SJSF = jsmxRow.getAs[String]("SJSF")
      val JGDM = jsmxRow.getAs[String]("JGDM")
      val FJSM = jsmxRow.getAs[String]("FJSM")

      //证券标识
      val ZQBS = "ZQ"
      //交易类别
      var JYLB = "XZLJ_SFHG"

      val QTRQ2 = wdqRow.getAs[String]("QTRQ").trim
      val CJRQ2 = wdqRow.getAs[String]("CJRQ").trim
      val days: Long = DateUtils.absDays(QTRQ2, CJRQ2)
      //qtrq-cjrq
      var QTRQCJRQ = days.toString

      SFHGETL(SCDM, JLLX, JYFS, JSFS, YWLX, QSBZ, GHLX, JSBH, CJBH, SQBH, WTBH,
        JYRQ, QSRQ, JSRQ, QTRQ, WTSJ, CJSJ, XWH1, XWH2, XWHY, JSHY, TGHY, ZQZH, ZQDM1,
        ZQDM2, ZQLB, LTLX, QYLB, GPNF, MMBZ, SL, CJSL, ZJZH, BZ, JG1, JG2, QSJE, YHS,
        JSF, GHF, ZGF, SXF, QTJE1, QTJE2, QTJE3, SJSF, JGDM, FJSM,
        //证券标识
        ZQBS,
        //交易类别
        JYLB,
        //qtrq-cjrq
        QTRQCJRQ
      )
    })

    val xzxkRDD = jsmxAndWdqRDD.map(row => {
      val jsmxRow = row._2._1
      val wdqRow = row._2._2
      val SCDM = jsmxRow.getAs[String]("SCDM")
      val JLLX = jsmxRow.getAs[String]("JLLX")
      val JYFS = jsmxRow.getAs[String]("JYFS")
      val JSFS = jsmxRow.getAs[String]("JSFS")
      val YWLX = jsmxRow.getAs[String]("YWLX")
      val QSBZ = jsmxRow.getAs[String]("QSBZ")
      val GHLX = jsmxRow.getAs[String]("GHLX")
      val JSBH = jsmxRow.getAs[String]("JSBH")
      val CJBH = jsmxRow.getAs[String]("CJBH")
      val SQBH = jsmxRow.getAs[String]("SQBH")
      val WTBH = jsmxRow.getAs[String]("WTBH")
      val JYRQ = jsmxRow.getAs[String]("JYRQ")
      val QSRQ = jsmxRow.getAs[String]("QSRQ")
      val JSRQ = jsmxRow.getAs[String]("JSRQ")
      val QTRQ = jsmxRow.getAs[String]("QTRQ")
      val WTSJ = jsmxRow.getAs[String]("WTSJ")
      val CJSJ = jsmxRow.getAs[String]("CJSJ")
      val XWH1 = jsmxRow.getAs[String]("XWH1")
      val XWH2 = jsmxRow.getAs[String]("XWH2")
      val XWHY = jsmxRow.getAs[String]("XWHY")
      val JSHY = jsmxRow.getAs[String]("JSHY")
      val TGHY = jsmxRow.getAs[String]("TGHY")
      val ZQZH = jsmxRow.getAs[String]("ZQZH")
      val ZQDM1 = jsmxRow.getAs[String]("ZQDM1")
      val ZQDM2 = jsmxRow.getAs[String]("ZQDM2")
      val ZQLB = jsmxRow.getAs[String]("ZQLB")
      val LTLX = jsmxRow.getAs[String]("LTLX")
      val QYLB = jsmxRow.getAs[String]("QYLB")
      val GPNF = jsmxRow.getAs[String]("GPNF")
      val MMBZ = jsmxRow.getAs[String]("MMBZ")
      val SL = jsmxRow.getAs[String]("SL")
      val CJSL = jsmxRow.getAs[String]("CJSL")
      val ZJZH = jsmxRow.getAs[String]("ZJZH")
      val BZ = jsmxRow.getAs[String]("BZ")
      val JG1 = jsmxRow.getAs[String]("JG1")
      val JG2 = jsmxRow.getAs[String]("JG2")
      val QSJE = jsmxRow.getAs[String]("QSJE")
      val YHS = jsmxRow.getAs[String]("YHS")
      val JSF = jsmxRow.getAs[String]("JSF")
      val GHF = jsmxRow.getAs[String]("GHF")
      val ZGF = jsmxRow.getAs[String]("ZGF")
      val SXF = jsmxRow.getAs[String]("SXF")
      val QTJE1 = jsmxRow.getAs[String]("QTJE1")
      val QTJE2 = jsmxRow.getAs[String]("QTJE2")
      val QTJE3 = jsmxRow.getAs[String]("QTJE3")
      val SJSF = jsmxRow.getAs[String]("SJSF")
      val JGDM = jsmxRow.getAs[String]("JGDM")
      val FJSM = jsmxRow.getAs[String]("FJSM")

      //证券标识
      val ZQBS = "ZQ"
      //交易类别
      var JYLB = "XZXK_SFHG"

      val QTRQ2 = wdqRow.getAs[String]("QTRQ").trim
      val CJRQ2 = wdqRow.getAs[String]("CJRQ").trim
      val days: Long = DateUtils.absDays(QTRQ2, CJRQ2)
      //qtrq-cjrq
      var QTRQCJRQ = days.toString

      SFHGETL(SCDM, JLLX, JYFS, JSFS, YWLX, QSBZ, GHLX, JSBH, CJBH, SQBH, WTBH,
        JYRQ, QSRQ, JSRQ, QTRQ, WTSJ, CJSJ, XWH1, XWH2, XWHY, JSHY, TGHY, ZQZH, ZQDM1,
        ZQDM2, ZQLB, LTLX, QYLB, GPNF, MMBZ, SL, CJSL, ZJZH, BZ, JG1, JG2, QSJE, YHS,
        JSF, GHF, ZGF, SXF, QTJE1, QTJE2, QTJE3, SJSF, JGDM, FJSM,
        //证券标识
        ZQBS,
        //交易类别
        JYLB,
        //qtrq-cjrq
        QTRQCJRQ
      )
    })
    xzljRDD.union(xzxkRDD)

  }

  private def jsmx013Entity(jsmx013FiltedRDD: RDD[Row]) = {
    jsmx013FiltedRDD.map(row => {
      val SCDM = row.getAs[String]("SCDM")
      val JLLX = row.getAs[String]("JLLX")
      val JYFS = row.getAs[String]("JYFS")
      val JSFS = row.getAs[String]("JSFS")
      val YWLX = row.getAs[String]("YWLX")
      val QSBZ = row.getAs[String]("QSBZ")
      val GHLX = row.getAs[String]("GHLX")
      val JSBH = row.getAs[String]("JSBH")
      val CJBH = row.getAs[String]("CJBH")
      val SQBH = row.getAs[String]("SQBH")
      val WTBH = row.getAs[String]("WTBH")
      val JYRQ = row.getAs[String]("JYRQ")
      val QSRQ = row.getAs[String]("QSRQ")
      val JSRQ = row.getAs[String]("JSRQ")
      val QTRQ = row.getAs[String]("QTRQ")
      val WTSJ = row.getAs[String]("WTSJ")
      val CJSJ = row.getAs[String]("CJSJ")
      val XWH1 = row.getAs[String]("XWH1")
      val XWH2 = row.getAs[String]("XWH2")
      val XWHY = row.getAs[String]("XWHY")
      val JSHY = row.getAs[String]("JSHY")
      val TGHY = row.getAs[String]("TGHY")
      val ZQZH = row.getAs[String]("ZQZH")
      val ZQDM1 = row.getAs[String]("ZQDM1")
      val ZQDM2 = row.getAs[String]("ZQDM2")
      val ZQLB = row.getAs[String]("ZQLB")
      val LTLX = row.getAs[String]("LTLX")
      val QYLB = row.getAs[String]("QYLB")
      val GPNF = row.getAs[String]("GPNF")
      val MMBZ = row.getAs[String]("MMBZ")
      val SL = row.getAs[String]("SL")
      val CJSL = row.getAs[String]("CJSL")
      val ZJZH = row.getAs[String]("ZJZH")
      val BZ = row.getAs[String]("BZ")
      val JG1 = row.getAs[String]("JG1")
      val JG2 = row.getAs[String]("JG2")
      val QSJE = row.getAs[String]("QSJE")
      val YHS = row.getAs[String]("YHS")
      val JSF = row.getAs[String]("JSF")
      val GHF = row.getAs[String]("GHF")
      val ZGF = row.getAs[String]("ZGF")
      val SXF = row.getAs[String]("SXF")
      val QTJE1 = row.getAs[String]("QTJE1")
      val QTJE2 = row.getAs[String]("QTJE2")
      val QTJE3 = row.getAs[String]("QTJE3")
      val SJSF = row.getAs[String]("SJSF")
      val JGDM = row.getAs[String]("JGDM")
      val FJSM = row.getAs[String]("FJSM")

      //证券标识
      val ZQBS = "ZQ"
      //交易类别
      var JYLB = " "
      //qtrq-cjrq
      var QTRQCJRQ = "0"

      YWLX match {
        case "680" => JYLB = "CS_SFHG"
        case "681" => JYLB = "DQ_SFHG"
        case "683" => JYLB = "TQGH_SFHG"
        case _ => JYLB = " "
      }

      SFHGETL(SCDM, JLLX, JYFS, JSFS, YWLX, QSBZ, GHLX, JSBH, CJBH, SQBH, WTBH,
        JYRQ, QSRQ, JSRQ, QTRQ, WTSJ, CJSJ, XWH1, XWH2, XWHY, JSHY, TGHY, ZQZH, ZQDM1,
        ZQDM2, ZQLB, LTLX, QYLB, GPNF, MMBZ, SL, CJSL, ZJZH, BZ, JG1, JG2, QSJE, YHS,
        JSF, GHF, ZGF, SXF, QTJE1, QTJE2, QTJE3, SJSF, JGDM, FJSM,
        //证券标识
        ZQBS,
        //交易类别
        JYLB,
        //qtrq-cjrq
        QTRQCJRQ
      )

    })
  }

  /**
    * 读取jsmx文件并过滤出满足条件的数据
    *
    * @param spark
    * @param jsmxFilePath
    * @return
    */
  private def readJsmxFileAndFilted(spark: SparkSession, jsmxFilePath: String): RDD[Row] = {
    val jsmxRDD: RDD[Row] = spark.sqlContext.dbfFile(jsmxFilePath).rdd

    jsmxRDD.filter(row => {
      val JLLX = row.getAs[String]("JLLX").trim
      val JYFS = row.getAs[String]("JYFS").trim
      val YWLX = row.getAs[String]("YWLX").trim
      val JGDM = row.getAs[String]("JGDM").trim
      "003".equals(JLLX) && "106".equals(JYFS) && ("680".equals(YWLX) || "681".equals(YWLX) || "682".equals(YWLX) || "683".equals(YWLX)) && "0000".equals(JGDM)
    })


  }


  private def readWdqFileAndFilted(spark: SparkSession, wdqFilePath: String): RDD[Row] = {
    val wdqRDD: RDD[Row] = spark.sqlContext.dbfFile(wdqFilePath).rdd
    //过滤数据,wdq（未到期）文件中：scdm=‘01’and wdqlb=‘008’的所有数据
    wdqRDD.filter(row => {
      val SCDM = row.getAs[String]("SCDM")
      val WDQLB = row.getAs[String]("WDQLB")
      "01".equals(SCDM) && "008".equals(WDQLB)
    })
  }


  /**
    * 读取jsmx文件并过滤出满足条件的数据
    * 过滤条件:
    * jsmx03（结算明细）文件中JLLX='003'（非担保交收业务的交收结果记录）
    * and JYFS='106'（RTGS 债券交易）and  YWLX in ('680','681','682','683') and JGDM = '0000'（正常交收）的数据
    *
    * @param spark
    */
  private def readFileAndFilted(spark: SparkSession, jsmxFilePath: String, wdqFilePath: String): Unit = {
    import com.yss.scala.dbf.dbf._
    val jsmxRDD: RDD[Row] = spark.sqlContext.dbfFile(jsmxFilePath).rdd
    val jsmxFiltedRDD = jsmxRDD.filter(row => {
      val JLLX = row.getAs[String]("JLLX").trim
      val JYFS = row.getAs[String]("JYFS").trim
      val YWLX = row.getAs[String]("YWLX").trim
      val JGDM = row.getAs[String]("JGDM").trim
      "003".equals(JLLX) && "106".equals(JYFS) && ("680".equals(YWLX) || "681".equals(YWLX) || "682".equals(YWLX) || "683".equals(YWLX)) && "0000".equals(JGDM)
    })

    val jsmx013FiltedRDD = jsmxFiltedRDD.filter(row => {
      val YWLX = row.getAs[String]("YWLX")
      "680".equals(YWLX) || "681".equals(YWLX) || "683".equals(YWLX)
    })

    val jsmx2FiltedRDD: RDD[Row] = jsmxFiltedRDD.subtract(jsmx013FiltedRDD)

    val wdqRDD: RDD[Row] = spark.sqlContext.dbfFile(wdqFilePath).rdd

    //过滤数据,wdq（未到期）文件中：scdm=‘01’and wdqlb=‘008’的所有数据
    val wdqFiltedRDD = wdqRDD.filter(row => {
      val SCDM = row.getAs[String]("SCDM")
      val WDQLB = row.getAs[String]("WDQLB")
      "01".equals(SCDM) && "008".equals(WDQLB)
    })

  }


  private case class SFHGETL(
                              SCDM: String,
                              JLLX: String,
                              JYFS: String,
                              JSFS: String,
                              YWLX: String,
                              QSBZ: String,
                              GHLX: String,
                              JSBH: String,
                              CJBH: String,
                              SQBH: String,
                              WTBH: String,
                              JYRQ: String,
                              QSRQ: String,
                              JSRQ: String,
                              QTRQ: String,
                              WTSJ: String,
                              CJSJ: String,
                              XWH1: String,
                              XWH2: String,
                              XWHY: String,
                              JSHY: String,
                              TGHY: String,
                              ZQZH: String,
                              ZQDM1: String,
                              ZQDM2: String,
                              ZQLB: String,
                              LTLX: String,
                              QYLB: String,
                              GPNF: String,
                              MMBZ: String,
                              SL: String,
                              CJSL: String,
                              ZJZH: String,
                              BZ: String,
                              JG1: String,
                              JG2: String,
                              QSJE: String,
                              YHS: String,
                              JSF: String,
                              GHF: String,
                              ZGF: String,
                              SXF: String,
                              QTJE1: String,
                              QTJE2: String,
                              QTJE3: String,
                              SJSF: String,
                              JGDM: String,
                              FJSM: String,
                              //证券标识
                              ZQBS: String,
                              //交易类别
                              JYLB: String,
                              //qtrq-cjrq
                              QTRQCJRQ: String
                            )

}
