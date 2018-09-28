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
      //.master("local[*]")
      .config("user", "hadoop")
      .getOrCreate()

    val day = DateUtils.formatDate(System.currentTimeMillis())
    val jsmxpath = "hdfs://192.168.102.120:8020/yss/guzhi/interface/" + day + "/jsmx03_jsjc1.528.csv"
    val wdqpath = "hdfs://192.168.102.120:8020/yss/guzhi/interface/" + day + "/wdqjsjc1.528.csv"
    val jsmxRDD: RDD[Row] = readJsmxFileAndFilted(spark, jsmxpath)

    val jsmx013FiltedRDD = jsmxRDD.filter(row => {
      val YWLX = getRowFieldAsString(row, "YWLX")
      "680".equals(YWLX) || "681".equals(YWLX) || "683".equals(YWLX)
    })

    val jsmx013SFHGETL: RDD[SHFICCTriPartyRepoETLDto] = jsmx013Entity(jsmx013FiltedRDD)

    val jsmx2RDD: RDD[Row] = jsmxRDD.subtract(jsmx013FiltedRDD)
    val wdqRDD: RDD[Row] = readWdqFileAndFilted(spark, wdqpath)

    val jsmx2SFHGETL: RDD[SHFICCTriPartyRepoETLDto] = jsmx2Entity(jsmx2RDD, wdqRDD)

    val res = jsmx013SFHGETL.union(jsmx2SFHGETL)

    val path = "hdfs://192.168.13.110:9000/guzhi/etl/sfgu/" + day


    val fs = FileSystem.get(new URI("hdfs://192.168.13.110:9000"), spark.sparkContext.hadoopConfiguration)
    if (fs.exists(new Path(path))) {
      fs.delete(new Path(path), true)
    }

    res.collect().foreach(println(_))
    saveAsCSV(spark, res, path)
    //saveMySQL(spark, res, path)

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
      val CJBH = getRowFieldAsString(row, "CJBH")
      val ZQZH = getRowFieldAsString(row, "ZQZH")
      val ZQDM1 = getRowFieldAsString(row, "ZQDM1")
      val XWH1 = getRowFieldAsString(row, "XWH1")
      (CJBH + ZQZH + ZQDM1 + XWH1, row)
    })

    val keyWDQRDD = wdqRDD.map(row => {
      val CJXLH = getRowFieldAsString(row, "CJXLH")
      val ZQZH = getRowFieldAsString(row, "ZQZH")
      val ZQDM = getRowFieldAsString(row, "ZQDM")
      val XWH1 = getRowFieldAsString(row, "XWH1")
      (CJXLH + ZQZH + ZQDM + XWH1, row)
    })

    val jsmxAndWdqRDD: RDD[(String, (Row, Row))] = keyJSMXRDD.join(keyWDQRDD)
    jsmxAndWdqRDD.persist()

    val xzljRDD = jsmxAndWdqRDD.map(row => {
      val jsmxRow = row._2._1
      val wdqRow = row._2._2
      val SCDM = getRowFieldAsString(jsmxRow, "SCDM")
      val JLLX = getRowFieldAsString(jsmxRow, "JLLX")
      val JYFS = getRowFieldAsString(jsmxRow, "JYFS")
      val JSFS = getRowFieldAsString(jsmxRow, "JSFS")
      val YWLX = getRowFieldAsString(jsmxRow, "YWLX")
      val QSBZ = getRowFieldAsString(jsmxRow, "QSBZ")
      val GHLX = getRowFieldAsString(jsmxRow, "GHLX")
      val JSBH = getRowFieldAsString(jsmxRow, "JSBH")
      val CJBH = getRowFieldAsString(jsmxRow, "CJBH")
      val SQBH = getRowFieldAsString(jsmxRow, "SQBH")
      val WTBH = getRowFieldAsString(jsmxRow, "WTBH")
      val JYRQ = getRowFieldAsString(jsmxRow, "JYRQ")
      val QSRQ = getRowFieldAsString(jsmxRow, "QSRQ")
      val JSRQ = getRowFieldAsString(jsmxRow, "JSRQ")
      val QTRQ = getRowFieldAsString(jsmxRow, "QTRQ")
      val WTSJ = getRowFieldAsString(jsmxRow, "WTSJ")
      val CJSJ = getRowFieldAsString(jsmxRow, "CJSJ")
      val XWH1 = getRowFieldAsString(jsmxRow, "XWH1")
      val XWH2 = getRowFieldAsString(jsmxRow, "XWH2")
      val XWHY = getRowFieldAsString(jsmxRow, "XWHY")
      val JSHY = getRowFieldAsString(jsmxRow, "JSHY")
      val TGHY = getRowFieldAsString(jsmxRow, "TGHY")
      val ZQZH = getRowFieldAsString(jsmxRow, "ZQZH")
      val ZQDM1 = getRowFieldAsString(jsmxRow, "ZQDM1")
      val ZQDM2 = getRowFieldAsString(jsmxRow, "ZQDM2")
      val ZQLB = getRowFieldAsString(jsmxRow, "ZQLB")
      val LTLX = getRowFieldAsString(jsmxRow, "LTLX")
      val QYLB = getRowFieldAsString(jsmxRow, "QYLB")
      val GPNF = getRowFieldAsString(jsmxRow, "GPNF")
      val MMBZ = getRowFieldAsString(jsmxRow, "MMBZ")
      val SL = getRowFieldAsString(jsmxRow, "SL")
      val CJSL = getRowFieldAsString(jsmxRow, "CJSL")
      val ZJZH = getRowFieldAsString(jsmxRow, "ZJZH")
      val BZ = getRowFieldAsString(jsmxRow, "BZ")
      val JG1 = getRowFieldAsString(jsmxRow, "JG1")
      val JG2 = getRowFieldAsString(jsmxRow, "JG2")
      val QSJE = getRowFieldAsString(jsmxRow, "QSJE")
      val YHS = getRowFieldAsString(jsmxRow, "YHS")
      val JSF = getRowFieldAsString(jsmxRow, "JSF")
      val GHF = getRowFieldAsString(jsmxRow, "GHF")
      val ZGF = getRowFieldAsString(jsmxRow, "ZGF")
      val SXF = getRowFieldAsString(jsmxRow, "SXF")
      val QTJE1 = getRowFieldAsString(jsmxRow, "QTJE1")
      val QTJE2 = getRowFieldAsString(jsmxRow, "QTJE2")
      val QTJE3 = getRowFieldAsString(jsmxRow, "QTJE3")
      val SJSF = getRowFieldAsString(jsmxRow, "SJSF")
      val JGDM = getRowFieldAsString(jsmxRow, "JGDM")
      val FJSM = getRowFieldAsString(jsmxRow, "FJSM")

      //证券标识
      val FZQBZ = "ZQ"
      //交易类别
      var FJYBZ = "XZLJ_SFHG"

      val QTRQ2 = getRowFieldAsString(wdqRow, "QTRQ")
      val CJRQ2 = getRowFieldAsString(wdqRow, "CJRQ")
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
      val SCDM = getRowFieldAsString(row, "SCDM")
      val JLLX = getRowFieldAsString(row, "JLLX")
      val JYFS = getRowFieldAsString(row, "JYFS")
      val JSFS = getRowFieldAsString(row, "JSFS")
      val YWLX = getRowFieldAsString(row, "YWLX")
      val QSBZ = getRowFieldAsString(row, "QSBZ")
      val GHLX = getRowFieldAsString(row, "GHLX")
      val JSBH = getRowFieldAsString(row, "JSBH")
      val CJBH = getRowFieldAsString(row, "CJBH")
      val SQBH = getRowFieldAsString(row, "SQBH")
      val WTBH = getRowFieldAsString(row, "WTBH")
      val JYRQ = getRowFieldAsString(row, "JYRQ")
      val QSRQ = getRowFieldAsString(row, "QSRQ")
      val JSRQ = getRowFieldAsString(row, "JSRQ")
      val QTRQ = getRowFieldAsString(row, "QTRQ")
      val WTSJ = getRowFieldAsString(row, "WTSJ")
      val CJSJ = getRowFieldAsString(row, "CJSJ")
      val XWH1 = getRowFieldAsString(row, "XWH1")
      val XWH2 = getRowFieldAsString(row, "XWH2")
      val XWHY = getRowFieldAsString(row, "XWHY")
      val JSHY = getRowFieldAsString(row, "JSHY")
      val TGHY = getRowFieldAsString(row, "TGHY")
      val ZQZH = getRowFieldAsString(row, "ZQZH")
      val ZQDM1 = getRowFieldAsString(row, "ZQDM1")
      val ZQDM2 = getRowFieldAsString(row, "ZQDM2")
      val ZQLB = getRowFieldAsString(row, "ZQLB")
      val LTLX = getRowFieldAsString(row, "LTLX")
      val QYLB = getRowFieldAsString(row, "QYLB")
      val GPNF = getRowFieldAsString(row, "GPNF")
      val MMBZ = getRowFieldAsString(row, "MMBZ")
      val SL = getRowFieldAsString(row, "SL")
      val CJSL = getRowFieldAsString(row, "CJSL")
      val ZJZH = getRowFieldAsString(row, "ZJZH")
      val BZ = getRowFieldAsString(row, "BZ")
      val JG1 = getRowFieldAsString(row, "JG1")
      val JG2 = getRowFieldAsString(row, "JG2")
      val QSJE = getRowFieldAsString(row, "QSJE")
      val YHS = getRowFieldAsString(row, "YHS")
      val JSF = getRowFieldAsString(row, "JSF")
      val GHF = getRowFieldAsString(row, "GHF")
      val ZGF = getRowFieldAsString(row, "ZGF")
      val SXF = getRowFieldAsString(row, "SXF")
      val QTJE1 = getRowFieldAsString(row, "QTJE1")
      val QTJE2 = getRowFieldAsString(row, "QTJE2")
      val QTJE3 = getRowFieldAsString(row, "QTJE3")
      val SJSF = getRowFieldAsString(row, "SJSF")
      val JGDM = getRowFieldAsString(row, "JGDM")
      val FJSM = getRowFieldAsString(row, "FJSM")

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
      val JLLX = getRowFieldAsString(row, "JLLX")
      val JYFS = getRowFieldAsString(row, "JYFS")
      val YWLX = getRowFieldAsString(row, "YWLX")
      val JGDM = getRowFieldAsString(row, "JGDM")
      "003".equals(JLLX) && "106".equals(JYFS) && ("680".equals(YWLX) || "681".equals(YWLX) || "682".equals(YWLX) || "683".equals(YWLX)) && "0000".equals(JGDM)
    })


  }


  private def readWdqFileAndFilted(spark: SparkSession, wdqFilePath: String): RDD[Row] = {
    val wdqRDD: RDD[Row] = Util.readCSV(wdqFilePath, spark).rdd
    //过滤数据,wdq（未到期）文件中：scdm=‘01’and wdqlb=‘008’的所有数据
    wdqRDD.filter(row => {
      val SCDM = getRowFieldAsString(row, "SCDM")
      val WDQLB = getRowFieldAsString(row, "WDQLB")
      "01".equals(SCDM) && "008".equals(WDQLB)
    })
  }


  private def getRowFieldAsString(row: Row, fieldName: String): String = {
    var field = row.getAs[String](fieldName)
    if (field == null) {
      field = ""
    } else {
      field = field.trim
    }
    field
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
