package com.yss.scala.guzhi

import com.yss.scala.util.{DateUtils, Util}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.math.BigDecimal.RoundingMode

/**
  * @auther: lijiayan
  * @date: 2018/9/6
  * @desc: 固定收益平台三方回购业务
  */
object GDSYSFHG {

  case class GDSY(
                   FDate: String,
                   FInDate: String,
                   FZqdm: String,
                   FSzsh: String,
                   FJyxwh: String,
                   Fje: String,
                   Fyj: String,
                   Fjsf: String,
                   FHggain: String,
                   Fsssfje: String,
                   FZqbz: String,
                   Fjybz: String,
                   ZqDm: String,
                   FJyFs: String,
                   Fsh: String,
                   Fzzr: String,
                   Fchk: String,
                   FHTXH: String,
                   FSETCODE: String,
                   FCSGHQX: String,
                   FRZLV: String,
                   FSJLY: String,
                   FCSHTXH: String,
                   FBS: String,
                   FSL: String,
                   Fyhs: String,
                   Fzgf: String,
                   Fghf: String,
                   FFxj: String,
                   FQtf: String,
                   Fgzlx: String,
                   FQsbz: String,
                   ftzbz: String,
                   FQsghf: String,
                   FGddm: String,
                   fzlh: String,
                   ISRTGS: String,
                   FPARTID: String,
                   FYwbz: String,
                   Fbz: String
                 )

  def main(args: Array[String]): Unit = {

    val jsmxFilePath = args(0)
    val wdqFilePath = args(1)

    val spark = SparkSession.builder()
      .appName(GDSYSFHG.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()


    //创建佣金临时表
    createYJLLTempTable(spark)

    import com.yss.scala.dbf.dbf._
    //读取结算明细文件jsmx.dbf
    val jsmxRDD: RDD[Row] = spark.sqlContext.dbfFile(jsmxFilePath).rdd


    //过滤数据,仅处理JLLX='003' and JYFS=‘106’ and YWLX in ('680','681','682','683') and JGDM = '0000'（正常交收）的数据
    val jsmxFiltedRDD = jsmxRDD.filter(row => {
      val JLLX = row.getAs[String]("JLLX").trim
      val JYFS = row.getAs[String]("JYFS").trim
      val YWLX = row.getAs[String]("YWLX").trim
      val JGDM = row.getAs[String]("JGDM").trim
      "003".equals(JLLX) && "106".equals(JYFS) && ("680".equals(YWLX) || "681".equals(YWLX) || "682".equals(YWLX) || "683".equals(YWLX)) && "0000".equals(JGDM)
    })
    jsmxFiltedRDD.persist(StorageLevel.MEMORY_ONLY)

    //过滤出YWLX为680,681,683的数据
    val jsmx013FiltedRDD = jsmxFiltedRDD.filter(row => {
      val YWLX = row.getAs[String]("YWLX")
      "680".equals(YWLX) || "681".equals(YWLX) || "683".equals(YWLX)
    })


    val jsmx013ResultRDD: RDD[GDSY] = jsmx013FiltedRDD.map(row => {

      val YWLX = row.getAs[String]("YWLX").trim

      val FDate = row.getAs[String]("JYRQ").trim
      var FInDate = ""
      var Fje = BigDecimal(row.getAs[String]("QSJE").trim).abs

      var Fyj = BigDecimal(0)

      var FCSGHQX: Long = 0L //到期日期-首期日期
      val FJyxwh = row.getAs[String]("XWH1").trim
      if ("680".equals(YWLX)) {
        FInDate = row.getAs[String]("QTRQ").trim

        //业务类型为680时，席位佣金=成交金额 * 佣金利率；
        Fyj = Fje.*(BigDecimal(getFV(spark, "GP", "S", FJyxwh)))
        /*.setScale(2, RoundingMode.HALF_UP)*/

        FCSGHQX = DateUtils.absDays(FInDate, FDate)


      } else {
        FInDate = FDate

        /*Fyj.setScale(2, RoundingMode.HALF_UP)*/

        FCSGHQX = DateUtils.absDays(FInDate, row.getAs[String]("QTRQ").trim)

        //回购金额=Abs(QsJe) / (1 + JG1 / 100 * 初始购回期限/ 365)
        Fje = Fje / (1 + BigDecimal(row.getAs[String]("JG1").trim) / 100 * FCSGHQX / 365)
      }


      val FZqdm = " "
      val FSzsh = "G"

      //Fje
      val Fjsf = BigDecimal(row.getAs[String]("JSF").trim).abs /*.setScale(2, RoundingMode.HALF_UP)*/

      //融资利率
      val FRZLV = BigDecimal(row.getAs[String]("JG1").trim)

      //Round（成交金额* JG1 / 100 *初始购回期限 / 365，2）  四舍五入
      val FHggain = Fje * FRZLV / 100 * FCSGHQX / 365
      val Fsssfje = BigDecimal(row.getAs[String]("SJSF").trim).abs /*.setScale(2, RoundingMode.HALF_UP)*/

      val FZqbz = "ZQ"

      var Fjybz = "CS_SFHG" //680
      if ("681".equals(YWLX)) {
        Fjybz = "DQ_SFHG"
      } else if ("683".equals(YWLX)) {
        Fjybz = "TQGH_SFHG"
      }

      val ZqDm = row.getAs[String]("ZQDM1").trim
      var FJyFs = ""
      val MMBZ = row.getAs[String]("MMBZ").trim
      if ("B".equals(MMBZ)) {
        FJyFs = "RZ"
      } else if ("S".equals(MMBZ)) {
        FJyFs = "RZ"
      }

      val Fsh = "1"

      val Fzzr = " " //TODO 怎么读到当前用户

      val Fchk = " " //TODO 怎么读取当前用户
      val FHTXH = row.getAs[String]("CJBH").trim

      val FSETCODE = " " //TODO 套账取值

      val FSJLY = "ZD"

      var FCSHTXH = row.getAs[String]("SQBH").trim
      if ("680".equals(YWLX)) {
        FCSHTXH = row.getAs[String]("CJBH").trim
      }

      val FBS = MMBZ
      val FSL = BigDecimal(row.getAs[String]("SL").trim)
      val Fyhs = BigDecimal(row.getAs[String]("YHS").trim)
      val Fzgf = BigDecimal(row.getAs[String]("ZGF").trim)
      val Fghf = BigDecimal(row.getAs[String]("GHF").trim)
      val FFxj = BigDecimal("0.00")
      val FQtf = BigDecimal("0.00")
      val Fgzlx = BigDecimal("0.00")
      val FQsbz = " "
      val ftzbz = " "
      val FQsghf = BigDecimal("0.00")
      val FGddm = " "
      val fzlh = " "
      val ISRTGS = " "
      val FPARTID = " "
      val FYwbz = " "
      val Fbz = " "

      GDSY(
        FDate,
        FInDate,
        FZqdm,
        FSzsh,
        FJyxwh,
        Fje.setScale(2, RoundingMode.HALF_UP).toString(),
        Fyj.setScale(2, RoundingMode.HALF_UP).toString(),
        Fjsf.setScale(2, RoundingMode.HALF_UP).toString(),
        FHggain.setScale(2, RoundingMode.HALF_UP).toString(),
        Fsssfje.setScale(2, RoundingMode.HALF_UP).toString(),
        FZqbz,
        Fjybz,
        ZqDm,
        FJyFs,
        Fsh,
        Fzzr,
        Fchk,
        FHTXH,
        FSETCODE,
        FCSGHQX.toString,
        FRZLV.setScale(4, RoundingMode.HALF_UP).toString(),
        FSJLY,
        FCSHTXH,
        FBS,
        FSL.setScale(2, RoundingMode.HALF_UP).toString(),
        Fyhs.setScale(2, RoundingMode.HALF_UP).toString(),
        Fzgf.setScale(2, RoundingMode.HALF_UP).toString(),
        Fghf.setScale(2, RoundingMode.HALF_UP).toString(),
        FFxj.setScale(2, RoundingMode.HALF_UP).toString(),
        FQtf.setScale(2, RoundingMode.HALF_UP).toString(),
        Fgzlx.setScale(2, RoundingMode.HALF_UP).toString(),
        FQsbz,
        ftzbz,
        FQsghf.setScale(2, RoundingMode.HALF_UP).toString(),
        FGddm,
        fzlh,
        ISRTGS,
        FPARTID,
        FYwbz,
        Fbz
      )
    })
    //jsmx013ResultRDD.collect().foreach(println(_))


    //求差集后得到682业务类型做特殊处理
    val jsmx2FiltedRDD: RDD[Row] = jsmxFiltedRDD.subtract(jsmx013FiltedRDD)

    jsmx2FiltedRDD.persist(StorageLevel.MEMORY_ONLY)

    val jsmx2JRDD = jsmx2FiltedRDD.map(row => {
      val CJBH = row.getAs[String]("CJBH").trim
      val ZQZH = row.getAs[String]("ZQZH").trim
      val ZQDM1 = row.getAs[String]("ZQDM1").trim
      val XWH1 = row.getAs[String]("XWH1").trim
      (CJBH + ZQZH + ZQDM1 + XWH1, row)
    })

    //读取未到期文件wdq.dbf
    val wdqRDD: RDD[Row] = spark.sqlContext.dbfFile(wdqFilePath).rdd

    //过滤数据,wdq（未到期）文件中：scdm=‘01’and wdqlb=‘008’的所有数据
    val wdqFiltedRDD = wdqRDD.filter(row => {
      val SCDM = row.getAs[String]("SCDM")
      val WDQLB = row.getAs[String]("WDQLB")
      "01".equals(SCDM) && "008".equals(WDQLB)
    })
    //wdqFiltedRDD.checkpoint()
    wdqFiltedRDD.persist()

    val wdqJRDD = wdqFiltedRDD.map(row => {
      val CJXLH = row.getAs[String]("CJXLH").trim
      val ZQZH = row.getAs[String]("ZQZH").trim
      val ZQDM = row.getAs[String]("ZQDM").trim
      val XWH1 = row.getAs[String]("XWH1").trim
      (CJXLH + ZQZH + ZQDM + XWH1, row)
    })
    val joinedRDD: RDD[(String, (Row, Row))] = jsmx2JRDD.join(wdqJRDD)


    //682续作合约新开数据取值规则
    val xzhyxkRDD = joinedRDD.map(item => {
      val row1 = item._2._1
      val row2 = item._2._2
      val FDate = row1.getAs[String]("JYRQ").trim
      val QTRQ = row2.getAs[String]("QTRQ").trim
      val CJRQ = row2.getAs[String]("CJRQ").trim
      val days: Long = DateUtils.absDays(QTRQ, CJRQ)
      val FInDate = DateUtils.addDays(FDate, days.toInt)
      val FZqdm = " "
      val FSzsh = "G"
      val FJyxwh = row1.getAs[String]("XWH1").trim
      val Fje = BigDecimal(row1.getAs[String]("QSJE").trim).abs
      val Fyj = BigDecimal(0.00) //TODO 根据交易所回购计算佣金选项如果为true；成交金额 * 佣金利率（券商佣金利率页面维护的利率）；false：佣金为0；
      val Fjsf = BigDecimal(row1.getAs[String]("JSF").trim).abs
      val FCSGHQX = days
      val FHggain = Fje * BigDecimal(row1.getAs[String]("JG1").trim) / 100 * FCSGHQX / 365
      val Fsssfje = Fje
      val FZqbz = "ZQ"
      val Fjybz = "XZXK_SFHG"
      val ZqDm = row1.getAs[String]("ZQDM1").trim
      val MMBZ = row1.getAs[String]("MMBZ").trim
      var FJyFs = "RZ"
      if ("B".equals(MMBZ)) {
        FJyFs = "RZ"
      } else if ("S".equals(MMBZ)) {
        FJyFs = "CZ"
      }

      val Fsh = "1"
      val Fzzr = " " //TODO 当前用户
      val Fchk = " " //TODO 当前用户

      val FHTXH = row1.getAs[String]("CJBH").trim
      val FSETCODE = " " // TODO 套账取值

      val FRZLV = BigDecimal(row1.getAs[String]("JG1").trim)
      val FSJLY = "ZD"

      val FCSHTXH = row1.getAs[String]("SQBH").trim
      val FBS = MMBZ
      val FSL = BigDecimal(row1.getAs[String]("SL").trim)
      val Fyhs = BigDecimal(row1.getAs[String]("YHS").trim)
      val Fzgf = BigDecimal(row1.getAs[String]("ZGF").trim)
      val Fghf = BigDecimal(row1.getAs[String]("GHF").trim)
      val FFxj = BigDecimal("0.00")
      val FQtf = BigDecimal("0.00")
      val Fgzlx = BigDecimal("0.00")
      val FQsbz = " "
      val ftzbz = " "
      val FQsghf = BigDecimal("0.00")
      val FGddm = " "
      val fzlh = " "
      val ISRTGS = " "
      val FPARTID = " "
      val FYwbz = " "
      val Fbz = " "

      GDSY(
        FDate,
        FInDate,
        FZqdm,
        FSzsh,
        FJyxwh,
        Fje.setScale(2, RoundingMode.HALF_UP).toString(),
        Fyj.setScale(2, RoundingMode.HALF_UP).toString(),
        Fjsf.setScale(2, RoundingMode.HALF_UP).toString(),
        FHggain.setScale(2, RoundingMode.HALF_UP).toString(),
        Fsssfje.setScale(2, RoundingMode.HALF_UP).toString(),
        FZqbz,
        Fjybz,
        ZqDm,
        FJyFs,
        Fsh,
        Fzzr,
        Fchk,
        FHTXH,
        FSETCODE,
        FCSGHQX.toString,
        FRZLV.setScale(4, RoundingMode.HALF_UP).toString(),
        FSJLY,
        FCSHTXH,
        FBS,
        FSL.setScale(2, RoundingMode.HALF_UP).toString(),
        Fyhs.setScale(2, RoundingMode.HALF_UP).toString(),
        Fzgf.setScale(2, RoundingMode.HALF_UP).toString(),
        Fghf.setScale(2, RoundingMode.HALF_UP).toString(),
        FFxj.setScale(2, RoundingMode.HALF_UP).toString(),
        FQtf.setScale(2, RoundingMode.HALF_UP).toString(),
        Fgzlx.setScale(2, RoundingMode.HALF_UP).toString(),
        FQsbz,
        ftzbz,
        FQsghf.setScale(2, RoundingMode.HALF_UP).toString(),
        FGddm,
        fzlh,
        ISRTGS,
        FPARTID,
        FYwbz,
        Fbz
      )

    })


    //682续作前期合约了结数据取值规则
    val xzqqhyljRDD = joinedRDD.map(item => {
      val row1 = item._2._1
      val row2 = item._2._2
      val FDate = row1.getAs[String]("JYRQ").trim
      val FInDate = FDate
      val FZqdm = " "
      val FSzsh = "G"
      val FJyxwh = row1.getAs[String]("XWH1").trim
      val FCSGHQX = DateUtils.absDays(row2.getAs[String]("QTRQ").trim, row2.getAs[String]("CJRQ").trim)

      val Fje = BigDecimal(row1.getAs[String]("QTJE1").trim).abs / (1 + BigDecimal(row1.getAs[String]("JG1").trim).setScale(4, RoundingMode.HALF_UP) / 100 * FCSGHQX / 365)

      val Fyj = BigDecimal(0.00)
      val Fjsf = BigDecimal(0.00)

      val FHggain = Fje * BigDecimal("0.00025" /*TODO 这里是初始回购利率*/) / 100 * FCSGHQX / 365
      val Fsssfje = BigDecimal(row1.getAs[String]("QTJE1").trim).abs

      val FZqbz = "ZQ"
      val Fjybz = "XZLJ_SFHG"
      val ZqDm = row1.getAs[String]("ZQDM1").trim
      val MMBZ = row1.getAs[String]("MMBZ").trim
      var FJyFs = "RZ"
      if ("B".equals(MMBZ)) {
        FJyFs = "RZ"
      } else if ("S".equals(MMBZ)) {
        FJyFs = "CZ"
      }

      val Fsh = "1"
      val Fzzr = " " //TODO 当前用户
      val Fchk = " " //TODO 当前用户

      val FHTXH = row1.getAs[String]("CJBH").trim
      val FSETCODE = " " // TODO 套账取值

      val FRZLV = BigDecimal(row1.getAs[String]("JG1").trim)
      val FSJLY = "ZD"

      val FCSHTXH = row1.getAs[String]("SQBH").trim
      val FBS = MMBZ //TODO 是不是等于买卖标志
      val FSL = BigDecimal(row1.getAs[String]("SL").trim)
      val Fyhs = BigDecimal(row1.getAs[String]("YHS").trim)
      val Fzgf = BigDecimal(row1.getAs[String]("ZGF").trim)
      val Fghf = BigDecimal(row1.getAs[String]("GHF").trim)
      val FFxj = BigDecimal("0.00")
      val FQtf = BigDecimal("0.00")
      val Fgzlx = BigDecimal("0.00")
      val FQsbz = " "
      val ftzbz = " "
      val FQsghf = BigDecimal("0.00")
      val FGddm = " "
      val fzlh = " "
      val ISRTGS = " "
      val FPARTID = " "
      val FYwbz = " "
      val Fbz = " "

      GDSY(
        FDate,
        FInDate,
        FZqdm,
        FSzsh,
        FJyxwh,
        Fje.setScale(2, RoundingMode.HALF_UP).toString(),
        Fyj.setScale(2, RoundingMode.HALF_UP).toString(),
        Fjsf.setScale(2, RoundingMode.HALF_UP).toString(),
        FHggain.setScale(2, RoundingMode.HALF_UP).toString(),
        Fsssfje.setScale(2, RoundingMode.HALF_UP).toString(),
        FZqbz,
        Fjybz,
        ZqDm,
        FJyFs,
        Fsh,
        Fzzr,
        Fchk,
        FHTXH,
        FSETCODE,
        FCSGHQX.toString,
        FRZLV.setScale(4, RoundingMode.HALF_UP).toString(),
        FSJLY,
        FCSHTXH,
        FBS,
        FSL.setScale(2, RoundingMode.HALF_UP).toString(),
        Fyhs.setScale(2, RoundingMode.HALF_UP).toString(),
        Fzgf.setScale(2, RoundingMode.HALF_UP).toString(),
        Fghf.setScale(2, RoundingMode.HALF_UP).toString(),
        FFxj.setScale(2, RoundingMode.HALF_UP).toString(),
        FQtf.setScale(2, RoundingMode.HALF_UP).toString(),
        Fgzlx.setScale(2, RoundingMode.HALF_UP).toString(),
        FQsbz,
        ftzbz,
        FQsghf.setScale(2, RoundingMode.HALF_UP).toString(),
        FGddm,
        fzlh,
        ISRTGS,
        FPARTID,
        FYwbz,
        Fbz
      )

    })

    //求并集
    val resultRDD = jsmx013ResultRDD.union(xzhyxkRDD).union(xzqqhyljRDD)
    resultRDD.collect().foreach(println(_))

    spark.stop()
  }


  /**
    * 创建佣金利率临时表 FVTable
    * FZQLB  FSZSH  FLV XWH
    *
    * @param spark
    */
  def createYJLLTempTable(spark: SparkSession): Unit = {
    val date = DateUtils.formatDate(System.currentTimeMillis())
    val path = "hdfs://bj-rack001-hadoop002:8020/yss/guzhi/basic_list/" + date + "/A117CSYJLV/"
    //原始数据
    val originDF: DataFrame = Util.readCSV(path, spark, header = false)
    //创建原始数据表
    originDF.createTempView("originDataTable")
    //从原始数据表中查出我们需要的字段：FZQLB(1),FSZSH(2),FLV(3),FSTR1(6) 并注册费率表FVTable
    spark.sql("select originDataTable._c1 as FZQLB,originDataTable._c2 as FSZSH,originDataTable._c3 as FLV,originDataTable._c6 as XWH from originDataTable")
      .createTempView("FVTable")
  }


  /**
    *
    * @param spark SparkSession
    * @param FZQLB 证券类别：默认ZQZYSFHG
    * @param FSZSH 默认：G
    * @param XWH   席位号
    * @return 返回字符串类型的佣金
    */
  def getFV(spark: SparkSession, FZQLB: String = "ZQZYSFHG", FSZSH: String = "G", XWH: String): String = {
    val rows: Array[Row] = spark.sql(s"select FLV from FVTable where FZQLB='$FZQLB' and FSZSH='$FSZSH' and XWH='$XWH'")
      .rdd.collect()
    rows(0).getAs[String]("FLV")
  }


}
