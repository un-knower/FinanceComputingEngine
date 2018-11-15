package com.yss.scala.core

import java.io.File
import java.util.Properties

import com.yss.scala.dto.SHFICCTriPartyRepoETLDto
import com.yss.scala.util.{DateUtils, RowUtils, BasicUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
  * @auther: lijiayan
  * @date: 2018/9/13
  * @desc: 上海固定收益三方回购ETL
  *        源文件:jsmx和wdq
  *        目标文件:csv
  */
object ShFICCTriPartyRepoETL {

  //源数据文件所在的hdfs路径
  private val DATA_FILE_PATH = "hdfs://192.168.102.120:8020/yss/guzhi/interface/"

  def main(args: Array[String]): Unit = {


    if (args == null || args.length != 1) throw new IllegalArgumentException("参数错误")

    val spark = SparkSession.builder()
      .appName(ShFICCTriPartyRepoETL.getClass.getSimpleName)
      .master("local[*]")
      //.config("user", "hadoop")
      .getOrCreate()

    //val day = DateUtils.formatDate(System.currentTimeMillis())
    val day = args(1)


    var jsmxpath = DATA_FILE_PATH + day + File.separator + "jsmxjsjc03" + File.separator + "*"
    var wdqpath = DATA_FILE_PATH + day + File.separator + "wdq" + File.separator + "*"

    /*if (args != null && args.length == 2) {
      jsmxpath = args(0)
      wdqpath = args(1)
    }*/
    val jsmxRDD: RDD[Row] = readJsmxFileAndFilted(spark, jsmxpath)

    val jsmx013FiltedRDD = jsmxRDD.filter(row => {
      val YWLX = RowUtils.getRowFieldAsString(row, "YWLX")
      "680".equals(YWLX) || "681".equals(YWLX) || "683".equals(YWLX)
    })


    val jsmx013SFHGETL: RDD[SHFICCTriPartyRepoETLDto] = jsmx013Entity(jsmx013FiltedRDD)

    val jsmx2RDD: RDD[Row] = jsmxRDD.subtract(jsmx013FiltedRDD)

    val wdqRDD: RDD[Row] = readWdqFileAndFilted(spark, wdqpath)

    val jsmx2SFHGETL: RDD[SHFICCTriPartyRepoETLDto] = jsmx2Entity(jsmx2RDD, wdqRDD)

    val res = jsmx013SFHGETL.union(jsmx2SFHGETL)

    //val path = ETL_HDFS_PATH + day


    //    val fs = FileSystem.get(new URI(ETL_HDFS_NAMENODE), spark.sparkContext.hadoopConfiguration)
    //    if (fs.exists(new Path(path))) {
    //      fs.delete(new Path(path), true)
    //    }

    //res.collect().foreach(println(_))
    //saveAsCSV(spark, res, path)
    //saveMySQL(spark, res, path)
    import spark.implicits._
    ShFICCTriPartyRepo.exec(spark, res.toDF())
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
    properties.put("driver", "com.mysql.jdbc.Driver")
    resDF.toDF().write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://192.168.102.120:3306/JJCWGZ", "JSMX03_WDQ_ETL", properties)

  }

  private def jsmx2Entity(jsmx2RDD: RDD[Row], wdqRDD: RDD[Row]) = {

    val keyJSMXRDD: RDD[(String, Row)] = jsmx2RDD.map(row => {
      val CJBH = RowUtils.getRowFieldAsString(row, "CJBH")
      val ZQZH = RowUtils.getRowFieldAsString(row, "ZQZH")
      val ZQDM1 = RowUtils.getRowFieldAsString(row, "ZQDM1")
      val XWH1 = RowUtils.getRowFieldAsString(row, "XWH1")
      (CJBH + ZQZH + ZQDM1 + XWH1, row)
    })

    val keyWDQRDD = wdqRDD.map(row => {
      val CJXLH = RowUtils.getRowFieldAsString(row, "CJXLH")
      val ZQZH = RowUtils.getRowFieldAsString(row, "ZQZH")
      val ZQDM = RowUtils.getRowFieldAsString(row, "ZQDM")
      val XWH1 = RowUtils.getRowFieldAsString(row, "XWH1")
      (CJXLH + ZQZH + ZQDM + XWH1, row)
    })

    val jsmxAndWdqRDD: RDD[(String, (Row, Option[Row]))] = keyJSMXRDD.leftOuterJoin(keyWDQRDD)
    jsmxAndWdqRDD.persist()

    val xzljRDD = jsmxAndWdqRDD.map(row => {
      val jsmxRow = row._2._1

      val wdqRow = row._2._2.orNull
      val SCDM = RowUtils.getRowFieldAsString(jsmxRow, "SCDM")
      val JLLX = RowUtils.getRowFieldAsString(jsmxRow, "JLLX")
      val JYFS = RowUtils.getRowFieldAsString(jsmxRow, "JYFS")
      val JSFS = RowUtils.getRowFieldAsString(jsmxRow, "JSFS")
      val YWLX = RowUtils.getRowFieldAsString(jsmxRow, "YWLX")
      val QSBZ = RowUtils.getRowFieldAsString(jsmxRow, "QSBZ")
      val GHLX = RowUtils.getRowFieldAsString(jsmxRow, "GHLX")
      val JSBH = RowUtils.getRowFieldAsString(jsmxRow, "JSBH")
      val CJBH = RowUtils.getRowFieldAsString(jsmxRow, "CJBH")
      val SQBH = RowUtils.getRowFieldAsString(jsmxRow, "SQBH")
      val WTBH = RowUtils.getRowFieldAsString(jsmxRow, "WTBH")
      val JYRQ = RowUtils.getRowFieldAsString(jsmxRow, "JYRQ")
      val QSRQ = RowUtils.getRowFieldAsString(jsmxRow, "QSRQ")
      val JSRQ = RowUtils.getRowFieldAsString(jsmxRow, "JSRQ")
      val QTRQ = RowUtils.getRowFieldAsString(jsmxRow, "QTRQ")
      val WTSJ = RowUtils.getRowFieldAsString(jsmxRow, "WTSJ")
      val CJSJ = RowUtils.getRowFieldAsString(jsmxRow, "CJSJ")
      val XWH1 = RowUtils.getRowFieldAsString(jsmxRow, "XWH1")
      val XWH2 = RowUtils.getRowFieldAsString(jsmxRow, "XWH2")
      val XWHY = RowUtils.getRowFieldAsString(jsmxRow, "XWHY")
      val JSHY = RowUtils.getRowFieldAsString(jsmxRow, "JSHY")
      val TGHY = RowUtils.getRowFieldAsString(jsmxRow, "TGHY")
      val ZQZH = RowUtils.getRowFieldAsString(jsmxRow, "ZQZH")
      val ZQDM1 = RowUtils.getRowFieldAsString(jsmxRow, "ZQDM1")
      val ZQDM2 = RowUtils.getRowFieldAsString(jsmxRow, "ZQDM2")
      val ZQLB = RowUtils.getRowFieldAsString(jsmxRow, "ZQLB")
      val LTLX = RowUtils.getRowFieldAsString(jsmxRow, "LTLX")
      val QYLB = RowUtils.getRowFieldAsString(jsmxRow, "QYLB")
      val GPNF = RowUtils.getRowFieldAsString(jsmxRow, "GPNF")
      val MMBZ = RowUtils.getRowFieldAsString(jsmxRow, "MMBZ")
      val SL = RowUtils.getRowFieldAsString(jsmxRow, "SL")
      val CJSL = RowUtils.getRowFieldAsString(jsmxRow, "CJSL")
      val ZJZH = RowUtils.getRowFieldAsString(jsmxRow, "ZJZH")
      val BZ = RowUtils.getRowFieldAsString(jsmxRow, "BZ")
      val JG1 = RowUtils.getRowFieldAsString(jsmxRow, "JG1")
      val JG2 = RowUtils.getRowFieldAsString(jsmxRow, "JG2")
      val QSJE = RowUtils.getRowFieldAsString(jsmxRow, "QSJE")
      val YHS = RowUtils.getRowFieldAsString(jsmxRow, "YHS")
      val JSF = RowUtils.getRowFieldAsString(jsmxRow, "JSF")
      val GHF = RowUtils.getRowFieldAsString(jsmxRow, "GHF")
      val ZGF = RowUtils.getRowFieldAsString(jsmxRow, "ZGF")
      val SXF = RowUtils.getRowFieldAsString(jsmxRow, "SXF")
      val QTJE1 = RowUtils.getRowFieldAsString(jsmxRow, "QTJE1")
      val QTJE2 = RowUtils.getRowFieldAsString(jsmxRow, "QTJE2")
      val QTJE3 = RowUtils.getRowFieldAsString(jsmxRow, "QTJE3")
      val SJSF = RowUtils.getRowFieldAsString(jsmxRow, "SJSF")
      val JGDM = RowUtils.getRowFieldAsString(jsmxRow, "JGDM")
      val FJSM = RowUtils.getRowFieldAsString(jsmxRow, "FJSM")

      //证券标识
      val FZQBZ = "ZQ"
      //交易类别
      val FJYBZ = "XZLJ_SFHG"
      var QTRQCJRQ = "0"
      if (wdqRow != null) {
        val QTRQ2 = RowUtils.getRowFieldAsString(wdqRow, "QTRQ")
        val CJRQ2 = RowUtils.getRowFieldAsString(wdqRow, "CJRQ")
        val days: Long = DateUtils.absDays(QTRQ2, CJRQ2)
        //qtrq-cjrq
        QTRQCJRQ = days.toString
      }

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
      val SCDM = RowUtils.getRowFieldAsString(row, "SCDM")
      val JLLX = RowUtils.getRowFieldAsString(row, "JLLX")
      val JYFS = RowUtils.getRowFieldAsString(row, "JYFS")
      val JSFS = RowUtils.getRowFieldAsString(row, "JSFS")
      val YWLX = RowUtils.getRowFieldAsString(row, "YWLX")
      val QSBZ = RowUtils.getRowFieldAsString(row, "QSBZ")
      val GHLX = RowUtils.getRowFieldAsString(row, "GHLX")
      val JSBH = RowUtils.getRowFieldAsString(row, "JSBH")
      val CJBH = RowUtils.getRowFieldAsString(row, "CJBH")
      val SQBH = RowUtils.getRowFieldAsString(row, "SQBH")
      val WTBH = RowUtils.getRowFieldAsString(row, "WTBH")
      val JYRQ = RowUtils.getRowFieldAsString(row, "JYRQ")
      val QSRQ = RowUtils.getRowFieldAsString(row, "QSRQ")
      val JSRQ = RowUtils.getRowFieldAsString(row, "JSRQ")
      val QTRQ = RowUtils.getRowFieldAsString(row, "QTRQ")
      val WTSJ = RowUtils.getRowFieldAsString(row, "WTSJ")
      val CJSJ = RowUtils.getRowFieldAsString(row, "CJSJ")
      val XWH1 = RowUtils.getRowFieldAsString(row, "XWH1")
      val XWH2 = RowUtils.getRowFieldAsString(row, "XWH2")
      val XWHY = RowUtils.getRowFieldAsString(row, "XWHY")
      val JSHY = RowUtils.getRowFieldAsString(row, "JSHY")
      val TGHY = RowUtils.getRowFieldAsString(row, "TGHY")
      val ZQZH = RowUtils.getRowFieldAsString(row, "ZQZH")
      val ZQDM1 = RowUtils.getRowFieldAsString(row, "ZQDM1")
      val ZQDM2 = RowUtils.getRowFieldAsString(row, "ZQDM2")
      val ZQLB = RowUtils.getRowFieldAsString(row, "ZQLB")
      val LTLX = RowUtils.getRowFieldAsString(row, "LTLX")
      val QYLB = RowUtils.getRowFieldAsString(row, "QYLB")
      val GPNF = RowUtils.getRowFieldAsString(row, "GPNF")
      val MMBZ = RowUtils.getRowFieldAsString(row, "MMBZ")
      val SL = RowUtils.getRowFieldAsString(row, "SL")
      val CJSL = RowUtils.getRowFieldAsString(row, "CJSL")
      val ZJZH = RowUtils.getRowFieldAsString(row, "ZJZH")
      val BZ = RowUtils.getRowFieldAsString(row, "BZ")
      val JG1 = RowUtils.getRowFieldAsString(row, "JG1")
      val JG2 = RowUtils.getRowFieldAsString(row, "JG2")
      val QSJE = RowUtils.getRowFieldAsString(row, "QSJE")
      val YHS = RowUtils.getRowFieldAsString(row, "YHS")
      val JSF = RowUtils.getRowFieldAsString(row, "JSF")
      val GHF = RowUtils.getRowFieldAsString(row, "GHF")
      val ZGF = RowUtils.getRowFieldAsString(row, "ZGF")
      val SXF = RowUtils.getRowFieldAsString(row, "SXF")
      val QTJE1 = RowUtils.getRowFieldAsString(row, "QTJE1")
      val QTJE2 = RowUtils.getRowFieldAsString(row, "QTJE2")
      val QTJE3 = RowUtils.getRowFieldAsString(row, "QTJE3")
      val SJSF = RowUtils.getRowFieldAsString(row, "SJSF")
      val JGDM = RowUtils.getRowFieldAsString(row, "JGDM")
      val FJSM = RowUtils.getRowFieldAsString(row, "FJSM")

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
    val jsmxRDD: RDD[Row] = BasicUtils.readCSV(jsmxFilePath, spark).rdd
    //val jsmxRDD: RDD[Row] = spark.sqlContext.dbfFile("/Users/lijiayan/Desktop/dbf/jsmx/jsmx03_jsjc1.802.1.dbf").rdd
    jsmxRDD.filter(row => {
      val JLLX = RowUtils.getRowFieldAsString(row, "JLLX")
      val JYFS = RowUtils.getRowFieldAsString(row, "JYFS")
      val YWLX = RowUtils.getRowFieldAsString(row, "YWLX")
      val JGDM = RowUtils.getRowFieldAsString(row, "JGDM")
      "003".equals(JLLX) && "106".equals(JYFS) && ("680".equals(YWLX) || "681".equals(YWLX) || "682".equals(YWLX) || "683".equals(YWLX)) && "0000".equals(JGDM)
    })


  }


  private def readWdqFileAndFilted(spark: SparkSession, wdqFilePath: String): RDD[Row] = {
    try {
      val wdqRDD: RDD[Row] = BasicUtils.readCSV(wdqFilePath, spark).rdd
      //val wdqRDD: RDD[Row]  = spark.sqlContext.dbfFile("/Users/lijiayan/Desktop/dbf/test").rdd
      //过滤数据,wdq（未到期）文件中：scdm=‘01’and wdqlb=‘008’的所有数据
      wdqRDD.filter(row => {
        val SCDM = RowUtils.getRowFieldAsString(row, "SCDM")
        val WDQLB = RowUtils.getRowFieldAsString(row, "WDQLB")
        "01".equals(SCDM) && "008".equals(WDQLB)
      })
    } catch {
      case e: Exception =>
        e.printStackTrace()
        spark.sparkContext.parallelize(Seq())

    }

  }

}
