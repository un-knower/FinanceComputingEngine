package com.yss.scala.guzhi

import java.net.URI
import java.util.Properties

import com.yss.scala.dto.SHFICCTriPartyRepoETLDto
import com.yss.scala.util.{DateUtils, Util}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
  * @auther: lijiayan
  * @date: 2018/9/13
  * @desc: 上海固定收益三方回购ETL
  *        源文件:jsmx和wdq
  *        目标文件:csv
  */
object SHFICCTriPartyRepoETL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(SHFICCTriPartyRepoETL.getClass.getSimpleName)
      .master("local[*]")
      .config("user", "hadoop")
      .getOrCreate()

    val jsmxpath = "hdfs://192.168.102.120:8020/yss/guzhi/interface/20180918/jsmx03_jsjc1.528.csv"
    val wdqpath = "hdfs://192.168.102.120:8020/yss/guzhi/interface/20180918/wdqjsjc1.528.csv"
    val jsmxRDD: RDD[Row] = readJsmxFileAndFilted(spark, jsmxpath)

    val jsmx013FiltedRDD = jsmxRDD.filter(row => {
      val YWLX = row.getAs[String]("YWLX").trim
      "680".equals(YWLX) || "681".equals(YWLX) || "683".equals(YWLX)
    })

    val jsmx013SFHGETL: RDD[SHFICCTriPartyRepoETLDto] = jsmx013Entity(jsmx013FiltedRDD)

    val jsmx2RDD: RDD[Row] = jsmxRDD.subtract(jsmx013FiltedRDD)
    val wdqRDD: RDD[Row] = readWdqFileAndFilted(spark, wdqpath)

    val jsmx2SFHGETL: RDD[SHFICCTriPartyRepoETLDto] = jsmx2Entity(jsmx2RDD, wdqRDD)

    val res = jsmx013SFHGETL.union(jsmx2SFHGETL)

    val path = "hdfs://192.168.13.110:9000/guzhi/etl/sfgu/" + DateUtils.formatDate(System.currentTimeMillis())


    val fs = FileSystem.get(new URI("hdfs://192.168.13.110:9000"), spark.sparkContext.hadoopConfiguration)
    if (fs.exists(new Path(path))) {
      fs.delete(new Path(path), true)
    }

    res.collect().foreach(println(_))
    saveAsCSV(spark, res, path)
    saveMySQL(spark, res, path)

    spark.stop()
  }


  private def saveAsCSV(spark: SparkSession, res: RDD[SHFICCTriPartyRepoETLDto], path: String): Unit = {
    import spark.implicits._
    res.coalesce(1).toDF(
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
      "FZQBZ",
      "FJYBZ",
      "QTRQCJRQ"
    ).write
      .format("csv")
      .option("header", value = true)
      .option("delimiter", ",")
      .option("charset", "UTF-8")
      .csv(path)
  }

  private def saveMySQL(spark: SparkSession, res: RDD[SHFICCTriPartyRepoETLDto], path: String): Unit = {
    import spark.implicits._
    val resDF = res.toDF(
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
      "FZQBZ",
      "FJYBZ",
      "QTRQCJRQ"
    )
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "root1234")
    resDF.toDF().write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://192.168.102.120:3306/JJCWGZ", "JSMX03_WDQ_ETL", properties)

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
      val SCDM = jsmxRow.getAs[String]("SCDM").trim
      val JLLX = jsmxRow.getAs[String]("JLLX").trim
      val JYFS = jsmxRow.getAs[String]("JYFS").trim
      val JSFS = jsmxRow.getAs[String]("JSFS").trim
      val YWLX = jsmxRow.getAs[String]("YWLX").trim
      val QSBZ = jsmxRow.getAs[String]("QSBZ").trim
      val GHLX = jsmxRow.getAs[String]("GHLX").trim
      val JSBH = jsmxRow.getAs[String]("JSBH").trim
      val CJBH = jsmxRow.getAs[String]("CJBH").trim
      val SQBH = jsmxRow.getAs[String]("SQBH").trim
      val WTBH = jsmxRow.getAs[String]("WTBH").trim
      val JYRQ = jsmxRow.getAs[String]("JYRQ").trim
      val QSRQ = jsmxRow.getAs[String]("QSRQ").trim
      val JSRQ = jsmxRow.getAs[String]("JSRQ").trim
      val QTRQ = jsmxRow.getAs[String]("QTRQ").trim
      val WTSJ = jsmxRow.getAs[String]("WTSJ").trim
      val CJSJ = jsmxRow.getAs[String]("CJSJ").trim
      val XWH1 = jsmxRow.getAs[String]("XWH1").trim
      val XWH2 = jsmxRow.getAs[String]("XWH2").trim
      val XWHY = jsmxRow.getAs[String]("XWHY").trim
      val JSHY = jsmxRow.getAs[String]("JSHY").trim
      val TGHY = jsmxRow.getAs[String]("TGHY").trim
      val ZQZH = jsmxRow.getAs[String]("ZQZH").trim
      val ZQDM1 = jsmxRow.getAs[String]("ZQDM1").trim
      val ZQDM2 = jsmxRow.getAs[String]("ZQDM2").trim
      val ZQLB = jsmxRow.getAs[String]("ZQLB").trim
      val LTLX = jsmxRow.getAs[String]("LTLX").trim
      val QYLB = jsmxRow.getAs[String]("QYLB").trim
      val GPNF = jsmxRow.getAs[String]("GPNF").trim
      val MMBZ = jsmxRow.getAs[String]("MMBZ").trim
      val SL = jsmxRow.getAs[String]("SL").trim
      val CJSL = jsmxRow.getAs[String]("CJSL").trim
      val ZJZH = jsmxRow.getAs[String]("ZJZH").trim
      val BZ = jsmxRow.getAs[String]("BZ").trim
      val JG1 = jsmxRow.getAs[String]("JG1").trim
      val JG2 = jsmxRow.getAs[String]("JG2").trim
      val QSJE = jsmxRow.getAs[String]("QSJE").trim
      val YHS = jsmxRow.getAs[String]("YHS").trim
      val JSF = jsmxRow.getAs[String]("JSF").trim
      val GHF = jsmxRow.getAs[String]("GHF").trim
      val ZGF = jsmxRow.getAs[String]("ZGF").trim
      val SXF = jsmxRow.getAs[String]("SXF").trim
      val QTJE1 = jsmxRow.getAs[String]("QTJE1").trim
      val QTJE2 = jsmxRow.getAs[String]("QTJE2").trim
      val QTJE3 = jsmxRow.getAs[String]("QTJE3").trim
      val SJSF = jsmxRow.getAs[String]("SJSF").trim
      val JGDM = jsmxRow.getAs[String]("JGDM").trim
      val FJSM = jsmxRow.getAs[String]("FJSM").trim

      //证券标识
      val FZQBZ = "ZQ"
      //交易类别
      var FJYBZ = "XZLJ_SFHG"

      val QTRQ2 = wdqRow.getAs[String]("QTRQ").trim
      val CJRQ2 = wdqRow.getAs[String]("CJRQ").trim
      val days: Long = DateUtils.absDays(QTRQ2, CJRQ2)
      //qtrq-cjrq
      var QTRQCJRQ = days.toString

      val sfhgetl: SHFICCTriPartyRepoETLDto = SHFICCTriPartyRepoETLDto(SCDM, JLLX, JYFS, JSFS, YWLX, QSBZ, GHLX, JSBH, CJBH, SQBH, WTBH,
        JYRQ, QSRQ, JSRQ, QTRQ, WTSJ, CJSJ, XWH1, XWH2, XWHY, JSHY, TGHY, ZQZH, ZQDM1,
        ZQDM2, ZQLB, LTLX, QYLB, GPNF, MMBZ, SL, CJSL, ZJZH, BZ, JG1, JG2, QSJE, YHS,
        JSF, GHF, ZGF, SXF, QTJE1, QTJE2, QTJE3, SJSF, JGDM, FJSM,
        //证券标识
        FZQBZ,
        //交易类别
        FJYBZ,
        //qtrq-cjrq
        QTRQCJRQ
      )
      val sfhgetl1 = sfhgetl.copy(FJYBZ = "XZXK_SFHG")
      Iterable(sfhgetl, sfhgetl1)
    })

    xzljRDD.flatMap(it => it.toList)
  }

  private def jsmx013Entity(jsmx013FiltedRDD: RDD[Row]) = {
    jsmx013FiltedRDD.map(row => {
      val SCDM = row.getAs[String]("SCDM").trim
      val JLLX = row.getAs[String]("JLLX").trim
      val JYFS = row.getAs[String]("JYFS").trim
      val JSFS = row.getAs[String]("JSFS").trim
      val YWLX = row.getAs[String]("YWLX").trim
      val QSBZ = row.getAs[String]("QSBZ").trim
      val GHLX = row.getAs[String]("GHLX").trim
      val JSBH = row.getAs[String]("JSBH").trim
      val CJBH = row.getAs[String]("CJBH").trim
      val SQBH = row.getAs[String]("SQBH").trim
      val WTBH = row.getAs[String]("WTBH").trim
      val JYRQ = row.getAs[String]("JYRQ").trim
      val QSRQ = row.getAs[String]("QSRQ").trim
      val JSRQ = row.getAs[String]("JSRQ").trim
      val QTRQ = row.getAs[String]("QTRQ").trim
      val WTSJ = row.getAs[String]("WTSJ").trim
      val CJSJ = row.getAs[String]("CJSJ").trim
      val XWH1 = row.getAs[String]("XWH1").trim
      val XWH2 = row.getAs[String]("XWH2").trim
      val XWHY = row.getAs[String]("XWHY").trim
      val JSHY = row.getAs[String]("JSHY").trim
      val TGHY = row.getAs[String]("TGHY").trim
      val ZQZH = row.getAs[String]("ZQZH").trim
      val ZQDM1 = row.getAs[String]("ZQDM1").trim
      val ZQDM2 = row.getAs[String]("ZQDM2").trim
      val ZQLB = row.getAs[String]("ZQLB").trim
      val LTLX = row.getAs[String]("LTLX").trim
      val QYLB = row.getAs[String]("QYLB").trim
      val GPNF = row.getAs[String]("GPNF").trim
      val MMBZ = row.getAs[String]("MMBZ").trim
      val SL = row.getAs[String]("SL").trim
      val CJSL = row.getAs[String]("CJSL").trim
      val ZJZH = row.getAs[String]("ZJZH").trim
      val BZ = row.getAs[String]("BZ").trim
      val JG1 = row.getAs[String]("JG1").trim
      val JG2 = row.getAs[String]("JG2").trim
      val QSJE = row.getAs[String]("QSJE").trim
      val YHS = row.getAs[String]("YHS").trim
      val JSF = row.getAs[String]("JSF").trim
      val GHF = row.getAs[String]("GHF").trim
      val ZGF = row.getAs[String]("ZGF").trim
      val SXF = row.getAs[String]("SXF").trim
      val QTJE1 = row.getAs[String]("QTJE1").trim
      val QTJE2 = row.getAs[String]("QTJE2").trim
      val QTJE3 = row.getAs[String]("QTJE3").trim
      val SJSF = row.getAs[String]("SJSF").trim
      val JGDM = row.getAs[String]("JGDM").trim
      val FJSM = row.getAs[String]("FJSM").trim

      //证券标识
      val FZQBZ = "ZQ"
      //交易类别
      var FJYBZ = " "
      //qtrq-cjrq
      var QTRQCJRQ = "0"

      YWLX match {
        case "680" => FJYBZ = "CS_SFHG"
        case "681" => FJYBZ = "DQ_SFHG"
        case "683" => FJYBZ = "TQGH_SFHG"
        case _ => FJYBZ = " "
      }

      SHFICCTriPartyRepoETLDto(
        SCDM,
        JLLX,
        JYFS,
        JSFS,
        YWLX,
        QSBZ,
        GHLX,
        JSBH,
        CJBH,
        SQBH,
        WTBH,
        JYRQ,
        QSRQ,
        JSRQ,
        QTRQ,
        WTSJ,
        CJSJ,
        XWH1,
        XWH2,
        XWHY,
        JSHY,
        TGHY,
        ZQZH,
        ZQDM1,
        ZQDM2,
        ZQLB,
        LTLX,
        QYLB,
        GPNF,
        MMBZ,
        SL,
        CJSL,
        ZJZH,
        BZ,
        JG1,
        JG2,
        QSJE,
        YHS,
        JSF,
        GHF,
        ZGF,
        SXF,
        QTJE1,
        QTJE2,
        QTJE3,
        SJSF,
        JGDM,
        FJSM,
        //证券标识
        FZQBZ,
        //交易类别
        FJYBZ,
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
    val jsmxRDD: RDD[Row] = Util.readCSV(jsmxFilePath, spark).rdd
    jsmxRDD.filter(row => {
      val JLLX = row.getAs[String]("JLLX").trim
      val JYFS = row.getAs[String]("JYFS").trim
      val YWLX = row.getAs[String]("YWLX").trim
      val JGDM = row.getAs[String]("JGDM").trim
      "003".equals(JLLX) && "106".equals(JYFS) && ("680".equals(YWLX) || "681".equals(YWLX) || "682".equals(YWLX) || "683".equals(YWLX)) && "0000".equals(JGDM)
    })


  }


  private def readWdqFileAndFilted(spark: SparkSession, wdqFilePath: String): RDD[Row] = {
    val wdqRDD: RDD[Row] = Util.readCSV(wdqFilePath, spark).rdd
    //过滤数据,wdq（未到期）文件中：scdm=‘01’and wdqlb=‘008’的所有数据
    wdqRDD.filter(row => {
      val SCDM = row.getAs[String]("SCDM").trim
      val WDQLB = row.getAs[String]("WDQLB").trim
      "01".equals(SCDM) && "008".equals(WDQLB)
    })
  }

  /*private case class SHFICCTriPartyRepoETLDto(
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
                              FZQBZ: String,
                              //交易类别
                              FJYBZ: String,
                              //qtrq-cjrq
                              QTRQCJRQ: String
                            )
*/
}
