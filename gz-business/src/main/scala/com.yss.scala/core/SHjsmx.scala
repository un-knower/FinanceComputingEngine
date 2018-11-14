package com.yss.scala.core

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.yss.scala.core.SHjsmx.{getFSETID, getLVARLISTValue, getYWDate}
import com.yss.scala.core.ShghContants.{DEFAULT_VALUE_0, FSH, SEPARATE2, TABLE_NAME_HOLIDAY}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import com.yss.scala.dbf.dbf._
import com.yss.scala.dto.{Hzjkqs, SHjsmxHzjkqs}
import com.yss.scala.util.{DateUtils, Util}
import org.apache.spark
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.math.BigDecimal.RoundingMode
import scala.reflect.ClassTag

/**
  * @author MingZhang Wang
  * @version 2018-11-05 10:24
  *          describe: 上海交所明细
  *          目标文件：
  *          目标表：
  */

object SHjsmx {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SHjsmx")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sqlContext
    val sparkSession = sc.sparkSession

    //读取源文件
    val SHjsmxFileDF: DataFrame = Util.readCSV("hdfs://192.168.102.120:8020/yss/guzhi/interface/20181114/jsmxjs/jsmxjs20180108.dbf.tsv", spark)

    //读取CSGDZH表
    val CSGDZHTable: RDD[String] = sc.sparkContext.textFile("hdfs://192.168.102.120:8020/yss/guzhi/basic_list/20181114/CSGDZH/part-m-00000")
    //广播出去
    val CSGDZHBroadCast: Broadcast[Array[String]] = sc.sparkContext.broadcast(CSGDZHTable.collect())

    //读取LsetList表
    val LSETLISTTable: RDD[String] = sc.sparkContext.textFile("hdfs://192.168.102.120:8020/yss/guzhi/basic_list/20181114/LSETLIST/part-m-00000")
    //广播出去
    val LSETLISTBroadCast: Broadcast[Array[String]] = sc.sparkContext.broadcast(LSETLISTTable.collect())

    //读取LVARLIST表
    val LVARLISTTable: RDD[String] = sc.sparkContext.textFile("hdfs://192.168.102.120:8020/yss/guzhi/basic_list/20181114/LVARLIST/part-m-00000")
    //广播出去
    val LVARLISTBroadCast: Broadcast[Array[String]] = sc.sparkContext.broadcast(LVARLISTTable.collect())

    //读取CSQSFYLV表
    val CSQSFYLVTable: RDD[String] = sc.sparkContext.textFile("hdfs://192.168.102.120:8020/yss/guzhi/basic_list/20181114/CSQSFYLV/part-m-00000")
    //广播出去
    val CSQSFYLVBroadcast: Broadcast[Array[String]] = sc.sparkContext.broadcast(CSQSFYLVTable.collect())

    //读取CSZQXX表
    val CSZQXXTable: RDD[String] = sc.sparkContext.textFile("hdfs://192.168.102.120:8020/yss/guzhi/basic_list/20181114/CSZQXX/part-m-00000")
    // 广播出去
    val CSZQXXBroadCast: Broadcast[Array[String]] = sc.sparkContext.broadcast(CSZQXXTable.collect())

    //读取CSSYSYJLV表
    val CSSYSYJLVTable: RDD[String] = sc.sparkContext.textFile("hdfs://192.168.102.120:8020/yss/guzhi/basic_list/20181114/CSSYSYJLV/part-m-00000")
    //广播出去
    val CSSYSYJLVBroadCast: Broadcast[Array[String]] = sc.sparkContext.broadcast(CSSYSYJLVTable.collect())

    //读取CSSYSXWFY表
    val CSSYSXWFYTable: RDD[String] = sc.sparkContext.textFile("hdfs://192.168.102.120:8020/yss/guzhi/basic_list/20181114/CSSYSXWFY/part-m-00000")
    //广播出去
    val CSSYSXWFYBroadCast: Broadcast[Array[String]] = sc.sparkContext.broadcast(CSSYSXWFYTable.collect())

    //读取LSETCSSYSJJ表
    val LSETCSSYSJJTable: RDD[String] = sc.sparkContext.textFile("hdfs://192.168.102.120:8020/yss/guzhi/basic_list/20181107/LSETCSSYSJJ/part-m-00000")
    //广播出去
    val LSETCSSYSJJBroadCast: Broadcast[Array[String]] = sc.sparkContext.broadcast(LSETCSSYSJJTable.collect())

    //读取CSJYLV表
    val CSJYLVTable: RDD[String] = sc.sparkContext.textFile("hdfs://192.168.102.120:8020/yss/guzhi/basic_list/20181108/CSJYLV/part-m-00000")
    //广播出去
    val CSJYLVBroadCast: Broadcast[Array[String]] = sc.sparkContext.broadcast(CSJYLVTable.collect())


    //过滤原始数据
    val SHjsmxFileRDD: RDD[Row] = FilterDF2RDD(SHjsmxFileDF)

    val QSJEArr = SHjsmxFileRDD.filter(row => {
      "278".equals(row.getAs[String]("QSBZ"))
    }).collect()

    val QSJEBroadcast: Broadcast[Array[Row]] = sc.sparkContext.broadcast(QSJEArr)


    /**
      * CDR存托服务费ETL
      */
    val CDRCalculateRDD: RDD[Row] = SHjsmxFileRDD.filter(row => {
      ("360".equals(row.getAs[String]("YWLX")) && "C01".equals("QSBZ") && "003".equals("JYFS")) &&
        "20181106".equals(row.getAs[String]("QSRQ"))
    })
    if (CDRCalculateRDD != null) {
      CDRCalculate(sparkSession:SparkSession, CDRCalculateRDD: RDD[Row], CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]])
    }


    /**
      * 可转债、可交换债预发行；新股预发行，新股申购ETL
      */
    val XGSGCalculateRDD: RDD[Row] = SHjsmxFileRDD.filter(row => {
      var FSETID = ""
      try {
         FSETID = getFSETID(row.getAs[String]("ZQZH"), CSGDZHBroadCast, LSETLISTBroadCast)
      } catch {
        case exception: ArrayIndexOutOfBoundsException => println("csgdzh表或lsetList表数据无相关数据")
      }


      val blnZqQrValue: Array[String] = getLVARLISTValue(FSETID, "新股、新债申购仅T+3制作中签确认凭证", LVARLISTBroadCast)
      var blnZqQr: Boolean = false
      if (blnZqQrValue(1).equals("1")) {
        blnZqQr = true
      }

      ("035".equals(row.getAs[String]("YWLX")) &&
        "73A".equals(row.getAs[String]("QSBZ")) &&
        "01".equals(row.getAs[String]("SCDM")) &&
        "002".equals(row.getAs[String]("JSFS")) &&
        !(row.getAs[String]("ZQDM").startsWith("733") ||
          row.getAs[String]("ZQDM").startsWith("785") ||
          row.getAs[String]("ZQDM").startsWith("754") ||
          row.getAs[String]("ZQDM").startsWith("759"))) &&
        getYWDate.equals(row.getAs[String]("QSRQ")) &&
        "002".equals(row.getAs[String]("JLLX")) &&
        !blnZqQr
    })
    if (XGSGCalculateRDD != null) {
      XGSGCalculate(sparkSession: SparkSession, XGSGCalculateRDD: RDD[Row], CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]])
    }

    /**
      * 可转债、可交换债预发行；新股预发行，申购确认ETL
      */
    val SGQRCalculateRDD: RDD[Row] = SHjsmxFileRDD.filter(row => {
      ("035".equals(row.getAs[String]("YWLX")) &&
        "73A".equals(row.getAs[String]("QSBZ")) &&
        "01".equals(row.getAs[String]("SCDM")) &&
        "002".equals(row.getAs[String]("JSFS")) &&
        !(row.getAs[String]("ZQDM").startsWith("733") ||
          row.getAs[String]("ZQDM").startsWith("785") ||
          row.getAs[String]("ZQDM").startsWith("754") ||
          row.getAs[String]("ZQDM").startsWith("759"))) &&
        getYWDate.equals(row.getAs[String]("JSRQ")) &&
        "003".equals(row.getAs[String]("JLLX"))
    })
    if (SGQRCalculateRDD != null){
      SGQRCalculate(sparkSession: SparkSession, SGQRCalculateRDD: RDD[Row], CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]])
    }

    /**
      * 可转债、可交换债预发行；新股预发行，新债申购ETL
      */
    val XZSGCalculateRDD: RDD[Row] = SHjsmxFileRDD.filter(row => {

      val FSETID: String = getFSETID(row.getAs[String]("ZQZH"), CSGDZHBroadCast, LSETLISTBroadCast)
      val blnZqQrValue: Array[String] = getLVARLISTValue(FSETID, "新股、新债申购仅T+3制作中签确认凭证", LVARLISTBroadCast)
      var blnZqQr: Boolean = false
      if (blnZqQrValue(1).equals("1")) {
        blnZqQr = true
      }

      ("035".equals(row.getAs[String]("YWLX")) &&
        "73A".equals(row.getAs[String]("QSBZ")) &&
        "01".equals(row.getAs[String]("SCDM")) &&
        "002".equals(row.getAs[String]("JSFS")) &&
        (row.getAs[String]("ZQDM").startsWith("733") ||
          row.getAs[String]("ZQDM").startsWith("785") ||
          row.getAs[String]("ZQDM").startsWith("754") ||
          row.getAs[String]("ZQDM").startsWith("759"))) &&
        getYWDate.equals(row.getAs[String]("QSRQ")) &&
        "002".equals(row.getAs[String]("JLLX")) &&
        !blnZqQr
    })
    if (XZSGCalculateRDD != null){
      XZSGCalculate(sparkSession: SparkSession, XZSGCalculateRDD: RDD[Row], CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]])
    }

    /**
      * 可转债、可交换债预发行；新股预发行，新债申购确认ETL
      */
    val XZSGQRCalculateRDD: RDD[Row] = SHjsmxFileRDD.filter(row => {
      ("035".equals(row.getAs[String]("YWLX")) &&
        "73A".equals(row.getAs[String]("QSBZ")) &&
        "01".equals(row.getAs[String]("SCDM")) &&
        "002".equals(row.getAs[String]("JSFS")) &&
        (row.getAs[String]("ZQDM").startsWith("733") ||
          row.getAs[String]("ZQDM").startsWith("785") ||
          row.getAs[String]("ZQDM").startsWith("754") ||
          row.getAs[String]("ZQDM").startsWith("759"))) &&
        getYWDate.equals(row.getAs[String]("JSRQ")) &&
        "003".equals(row.getAs[String]("JLLX"))
    })
    if (XZSGQRCalculateRDD != null){
      XZSGQRCalculate(sparkSession: SparkSession, XZSGQRCalculateRDD: RDD[Row], CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]])
    }

    /**
      * 老股东配股配债-配股
      */
    val PGCalculateRDD: RDD[Row] = SHjsmxFileRDD.filter(row => {
      val FSETID: String = getFSETID(row.getAs[String]("ZQZH"), CSGDZHBroadCast, LSETLISTBroadCast)
      val tmp6Value: Array[String] = getLVARLISTValue(FSETID, "上交所质押式回购停牌", LVARLISTBroadCast)
      var blnShZytp = false
      if ("1".equals(tmp6Value(1)) && DateUtils.formattedDate2Long(tmp6Value(5), DateUtils.YYYYMMDD).compareTo(DateUtils.formattedDate2Long(getYWDate, DateUtils.YYYYMMDD)) >= 0) {
        blnShZytp = true
      }
      blnShZytp &&
        "012".equals(row.getAs[String]("YWLX")) &&
        "PG".equals(row.getAs[String]("ZQLB")) &&
        "070".equals(row.getAs[String]("QSBZ")) &&
        "01".equals(row.getAs[String]("SCDM")) &&
        (row.getAs[String]("ZQDM").startsWith("70") ||
          row.getAs[String]("ZQDM").startsWith("76")) &&
        !(row.getAs[String]("ZQDM").startsWith("704") ||
          row.getAs[String]("ZQDM").startsWith("762") ||
          row.getAs[String]("ZQDM").startsWith("764")) &&
        getYWDate.equals(row.getAs[String]("QSRQ"))
    })
    if (PGCalculateRDD != null){
      PGCalculate(sparkSession: SparkSession, PGCalculateRDD: RDD[Row], CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]])
    }
    /**
      * 老股东配股配债-配债
      */
    val PZCalculateRDD: RDD[Row] = SHjsmxFileRDD.filter { row => {
      val FSETID: String = getFSETID(row.getAs[String]("ZQZH"), CSGDZHBroadCast, LSETLISTBroadCast)
      val tmp6Value: Array[String] = getLVARLISTValue(FSETID, "上交所质押式回购停牌", LVARLISTBroadCast)
      var blnShZytp = false
      if ("1".equals(tmp6Value(1)) && DateUtils.formattedDate2Long(tmp6Value(5), DateUtils.YYYYMMDD).compareTo(DateUtils.formattedDate2Long(getYWDate, DateUtils.YYYYMMDD)) >= 0) {
        blnShZytp = true
      }
      blnShZytp &&
        "012".equals(row.getAs[String]("YWLX")) &&
        "PG".equals(row.getAs[String]("ZQLB")) &&
        "070".equals(row.getAs[String]("QSBZ")) &&
        "01".equals(row.getAs[String]("SCDM")) &&
        (row.getAs[String]("ZQDM").startsWith("704") ||
          row.getAs[String]("ZQDM").startsWith("762") ||
          row.getAs[String]("ZQDM").startsWith("764")) &&
        getYWDate.equals(row.getAs[String]("QSRQ"))

    }
    }
    if (PZCalculateRDD != null){
      PZCalculate(sparkSession: SparkSession, PZCalculateRDD: RDD[Row], CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]])
    }

    /**
      * 债转股、债换股-债转股债券项ETL
      */
    val ZZGZQCalculateRDD: RDD[Row] = SHjsmxFileRDD.filter(row => {

      val FSETID: String = getFSETID(row.getAs[String]("ZQZH"), CSGDZHBroadCast, LSETLISTBroadCast)
      val tmp3Value: Array[String] = getLVARLISTValue(FSETID, "上海证券交易所债转股业务数据使用上海结算明细数据启用日期(YYYYMMDD)", LVARLISTBroadCast)
      var bDoZzg = false
      if ("1".equals(tmp3Value(1)) && DateUtils.formattedDate2Long(tmp3Value(5), DateUtils.YYYYMMDD).compareTo(DateUtils.formattedDate2Long(getYWDate, DateUtils.YYYYMMDD)) >= 0) {
        bDoZzg = true
      }

      bDoZzg &&
        "011".equals(row.getAs[String]("YWLX")) &&
        "GZ".equals(row.getAs[String]("ZQLB")) &&
        !("278".equals(row.getAs[String]("QSBZ"))) &&
        getYWDate.equals(row.getAs[String]("QSRQ"))
    })
    if (ZZGZQCalculateRDD != null){
      ZZGZQCalculate(sparkSession: SparkSession, ZZGZQCalculateRDD: RDD[Row], CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]])
    }

    /**
      * 债转股、债换股-债转股股票项ETL
      */
    val ZZGGPCalculateRDD: RDD[Row] = SHjsmxFileRDD.filter(row => {
      val FSETID: String = getFSETID(row.getAs[String]("ZQZH"), CSGDZHBroadCast, LSETLISTBroadCast)
      val tmp3Value: Array[String] = getLVARLISTValue(FSETID, "上海证券交易所债转股业务数据使用上海结算明细数据启用日期(YYYYMMDD)", LVARLISTBroadCast)
      var bDoZzg = false
      if ("1".equals(tmp3Value(1)) && DateUtils.formattedDate2Long(tmp3Value(5), DateUtils.YYYYMMDD).compareTo(DateUtils.formattedDate2Long(getYWDate, DateUtils.YYYYMMDD)) >= 0) {
        bDoZzg = true
      }

      bDoZzg &&
        "011".equals(row.getAs[String]("YWLX")) &&
        "PT".equals(row.getAs[String]("ZQLB")) &&
        !("278".equals(row.getAs[String]("QSBZ"))) &&
        getYWDate.equals(row.getAs[String]("QSRQ"))
    })
    if (ZZGGPCalculateRDD != null){
      ZZGGPCalculate(sparkSession: SparkSession, ZZGGPCalculateRDD: RDD[Row], CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]], QSJEBroadcast: Broadcast[Array[Row]])
    }

    /**
      * 债转股、债换股-债转股债券项2ETL
      */
    val ZZGZQ2CalculateRDD: RDD[Row] = SHjsmxFileRDD.filter(row => {
      val FSETID: String = getFSETID(row.getAs[String]("ZQZH"), CSGDZHBroadCast, LSETLISTBroadCast)
      val tmp3Value: Array[String] = getLVARLISTValue(FSETID, "上海证券交易所债转股业务数据使用上海结算明细数据启用日期(YYYYMMDD)", LVARLISTBroadCast)
      var bDoZzg = false
      if ("1".equals(tmp3Value(1)) && DateUtils.formattedDate2Long(tmp3Value(5), DateUtils.YYYYMMDD).compareTo(DateUtils.formattedDate2Long(getYWDate, DateUtils.YYYYMMDD)) >= 0) {
        bDoZzg = true
      }

      bDoZzg &&
        "653".equals(row.getAs[String]("YWLX")) &&
        "S".equals(row.getAs[String]("")) &&
        "003".equals(row.getAs[String]("JLLX"))
      "GZ".equals(row.getAs[String]("ZQLB")) &&
        "179".equals(row.getAs[String]("QSBZ")) &&
        getYWDate.equals(row.getAs[String]("QSRQ"))
    })
    if (ZZGZQ2CalculateRDD != null){
      ZZGZQ2Calculate(sparkSession: SparkSession, ZZGZQ2CalculateRDD: RDD[Row], CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]])

    }

    /**
      * 债转股、债换股-债转股股票项2ETL
      */
    val ZZGGP2CalculateRDD: RDD[Row] = SHjsmxFileRDD.filter(row => {
      val FSETID: String = getFSETID(row.getAs[String]("ZQZH"), CSGDZHBroadCast, LSETLISTBroadCast)
      val tmp3Value: Array[String] = getLVARLISTValue(FSETID, "上海证券交易所债转股业务数据使用上海结算明细数据启用日期(YYYYMMDD)", LVARLISTBroadCast)
      var bDoZzg = false
      if ("1".equals(tmp3Value(1)) && DateUtils.formattedDate2Long(tmp3Value(5), DateUtils.YYYYMMDD).compareTo(DateUtils.formattedDate2Long(getYWDate, DateUtils.YYYYMMDD)) >= 0) {
        bDoZzg = true
      }

      bDoZzg &&
        "653".equals(row.getAs[String]("YWLX")) &&
        "S".equals(row.getAs[String]("")) &&
        "003".equals(row.getAs[String]("JLLX"))
      "PT".equals(row.getAs[String]("ZQLB")) &&
        "178".equals(row.getAs[String]("QSBZ")) &&
        getYWDate.equals(row.getAs[String]("QSRQ"))
    })
    if (ZZGGP2CalculateRDD != null){
      ZZGGP2Calculate(sparkSession: SparkSession, ZZGGP2CalculateRDD: RDD[Row], CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]])

    }

    /**
      * 要约收购、回售ETL-要约收购ETL
      */
    val YYSGCalculateRDD: RDD[Row] = SHjsmxFileRDD.filter(row => {
      row.getAs[String]("YWLX").contains("301") &&
        row.getAs[String]("ZQDM1").startsWith("6") &&
        getYWDate.equals(row.getAs[String]("QSRQ"))
    })
    if (YYSGCalculateRDD != null){
      YYSGCalculate(sparkSession: SparkSession, YYSGCalculateRDD: RDD[Row], CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]], LVARLISTBroadCast: Broadcast[Array[String]], CSQSFYLVBroadcast: Broadcast[Array[String]], CSSYSYJLVBroadCast: Broadcast[Array[String]])

    }

    /**
      * 要约收购、回售ETL-回售ETL
      */
    val HSCalculateRDD: RDD[Row] = SHjsmxFileRDD.filter(row => {
      row.getAs[String]("YWLX").contains("301") &&
        !(row.getAs[String]("ZQDM1").startsWith("6")) &&
        getYWDate.equals(row.getAs[String]("QSRQ"))
    })
    if (HSCalculateRDD != null){
      HSCalculate(sparkSession: SparkSession, HSCalculateRDD: RDD[Row], CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]], CSZQXXBroadCast: Broadcast[Array[String]], LVARLISTBroadCast: Broadcast[Array[String]])

    }

    /**
      * 回购-固定收益平台隔夜回购ETL
      */
    val GDSYGYHGCalculateRDD: RDD[Row] = SHjsmxFileRDD.filter(row => {
      "602".equals(row.getAs[String]("YWLX")) &&
        "001".equals(row.getAs[String]("JLLX")) &&
        getYWDate.equals(row.getAs[String]("QSRQ"))
    })
    if (GDSYGYHGCalculateRDD != null){
      GDSYGYHGCalculate(sparkSession: SparkSession, GDSYGYHGCalculateRDD: RDD[Row], CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]])

    }

    /**
      * 回购-固定收益平台隔夜回购到期ETL
      */
    val GDSYGYHGDQCalculateRDD: RDD[Row] = SHjsmxFileRDD.filter(row => {
      "603".equals(row.getAs[String]("YWLX")) &&
        "001".equals(row.getAs[String]("JLLX")) &&
        getYWDate.equals(row.getAs[String]("QSRQ"))
    })
    if (GDSYGYHGDQCalculateRDD != null){
      GDSYGYHGDQCalculate(sparkSession: SparkSession, GDSYGYHGDQCalculateRDD: RDD[Row], CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]])

    }

    /**
      * 回购-买入卖出回购ETL
      */
    val MRMCHGCalculateRDD: RDD[Row] = SHjsmxFileRDD.filter(row => {
      "117".equals(row.getAs[String]("YWLX")) &&
        "003".equals(row.getAs[String]("JLLX")) &&
        getYWDate.equals(row.getAs[String]("QSRQ"))
    })
    if (MRMCHGCalculateRDD != null){
  MRMCHGCalculate(sparkSession: SparkSession, MRMCHGCalculateRDD: RDD[Row], CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]], CSSYSYJLVBroadCast: Broadcast[Array[String]], LVARLISTBroadCast: Broadcast[Array[String]], CSSYSXWFYBroadCast: Broadcast[Array[String]], LSETCSSYSJJBroadCast: Broadcast[Array[String]], CSJYLVBroadCast: Broadcast[Array[String]])
    }
    sc.sparkSession.stop()

  }


  /**
    * CDR存托服务费计算规则
    *
    * @param CDRCalculateRDD
    * @param CSGDZHBroadCast
    * @param LSETLISTBroadCast
    */
  def CDRCalculate(sparkSession: SparkSession, CDRCalculateRDD: RDD[Row], CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]]): Unit = {
    val CDRTempRDD = CDRCalculateRDD.map(row => {
      val Zqzh: String = row.getAs[String]("ZQZH")
      //获取资产代码
      val FSETID: String = getFSETID(Zqzh, CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]])
      val Fdate: String = getYWDate
      val FinDate: String = getYWDate
      val Fzqdm: String = row.getAs[String]("ZQDM1")
      val Fszsh: String = "H"
      val Fjyxwh: String = row.getAs[String]("XWH1")
      val FBS: String = "B"
      val Fje = row.getAs[String]("QTJE1")
      val Fsl = row.getAs[String]("s1")
      val Fyj = "0"
      val Fjsf = getAbs(row, "JSF")
      val Fyhs = getAbs(row, "YHS")
      val Fzgf = getAbs(row, "ZHF")
      val Fghf = getAbs(row, "GHF")
      val Fgzlx = "0"
      val Fhggain = "0"
      val Ffxj = "0"

      val Fzqbz = "CDRGP"
      val Fywbz = "CDRGPFYZF"
      val Fqsbz = "N"
      val Fqtf = "0"
      val Zqdm = row.getAs[String]("ZQDM1")
      val Fjyfs = "FY"
      val Fsh = "1"
      val Fzzr = " "
      val Fchk = " "
      val Fzlh = "0"
      val Ftzbz = " "
      val Fqsghf = "0"
      val Fgddm = Zqzh
      val Fjybz = " "
      val Isrtgs = "1"
      val Fpartid = " "
      val Fhtxh = " "
      val Fcshtxh = " "
      val Frzlv = "0"
      val Fcsghqx = "0"
      val Fsjly = " "
      val Fbz = "RMB"
      val Fsssje = getFsssje(FBS: String, Fje: String, Fjsf: String, Fyhs: String, Fzgf: String, Fghf: String, Fqtf: String, Fqsghf: String) //TODO


      SHjsmxHzjkqs(
        FSETID: String,
        Fdate: String,
        FinDate: String,
        Fzqdm: String,
        Fszsh: String,
        Fjyxwh: String,
        FBS: String,
        Fje: String,
        Fyj: String,
        Fjsf: String,
        Fyhs: String,
        Fzgf: String,
        Fghf: String,
        Fgzlx: String,
        Fhggain: String,
        Ffxj: String,
        Fsssje: String,
        Fzqbz: String,
        Fywbz: String,
        Fqsbz: String,
        Fqtf: String,
        Zqdm: String,
        Fjyfs: String,
        Fsh: String,
        Fzzr: String,
        Fchk: String,
        Fzlh: String,
        Ftzbz: String,
        Fqsghf: String,
        Fgddm: String,
        Fjybz: String,
        Isrtgs: String,
        Fpartid: String,
        Fhtxh: String,
        Fcshtxh: String,
        Frzlv: String,
        Fcsghqx: String,
        Fsjly: String,
        Fbz: String
      )
    })
    save2Mysql(CDRTempRDD,sparkSession,"CDR_wmz")

  }

  /**
    * 可转债、可交换债预发行；新股预发行，新股申购计算规则
    *
    * @param sparkSession
    * @param XGSGCalculateRDD
    * @param CSGDZHBroadCast
    * @param LSETLISTBroadCast
    */
  def XGSGCalculate(sparkSession: SparkSession, XGSGCalculateRDD: RDD[Row], CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]]): Unit = {
    val XGSGTempRDD = XGSGCalculateRDD.map(row => {
      val Zqzh: String = row.getAs[String]("ZQZH")
      //获取资产代码
      val FSETID: String = getFSETID(Zqzh, CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]])
      val Fdate: String = getNextWorkDay(sparkSession, getYWDate)
      val FinDate: String = getYWDate

      var Fzqdm = ""
      val FzqdmTemp: String = row.getAs[String]("ZQDM1")
      if (FzqdmTemp.startsWith("712")) {
        Fzqdm = replaceString(FzqdmTemp, "609")
      } else if (FzqdmTemp.startsWith("734") || FzqdmTemp.startsWith("732")) {
        Fzqdm = replaceString(FzqdmTemp, "603")
      } else if (FzqdmTemp.startsWith("740") || FzqdmTemp.startsWith("730") || FzqdmTemp.startsWith("731")) {
        Fzqdm = replaceString(FzqdmTemp, "600")
      } else if (FzqdmTemp.startsWith("715")) {
        Fzqdm = replaceString(FzqdmTemp, "605")
      }

      val Fszsh: String = "H"
      val Fjyxwh: String = row.getAs[String]("XWH1")
      val FBS: String = "B"
      val Fje = mul(row.getAs[String]("sl"), row.getAs[String]("Jg1"), 2)
      val Fsl = row.getAs[String]("s1")
      val Fyj = "0"
      val Fjsf = getAbs(row, "JSF")
      val Fyhs = getAbs(row, "YHS")
      val Fzgf = getAbs(row, "ZHF")
      val Fghf = getAbs(row, "GHF")
      val Fgzlx = "0"
      val Fhggain = "0"
      val Ffxj = "0"

      val Fqsbz = "N"
      val Fqtf = "0"
      val Zqdm = row.getAs[String]("ZQDM1")
      val Fjyfs = "PT"
      val Fsh = "1"
      val Fzzr = " "
      val Fchk = " "
      val Fzlh = "0"
      val Ftzbz = " "
      val Fqsghf = "0"
      val Fgddm = Zqzh
      val Fjybz = " "
      val Isrtgs = "1"
      val Fpartid = " "
      val Fhtxh = " "
      val Fcshtxh = " "
      val Frzlv = "0"
      val Fcsghqx = "0"
      val Fsjly = " "
      val Fbz = "RMB"

      val Fsssje = getFsssje(FBS: String, Fje: String, Fjsf: String, Fyhs: String, Fzgf: String, Fghf: String, Fqtf: String, Fqsghf: String) //TODO
      var Fzqbz = getBz("712", Zqdm, "CDRGP", "XG")
      val Fywbz = getBz("712", Zqdm, "CDRGPPSZQ", "XGPSZQ")


      SHjsmxHzjkqs(
        FSETID: String,
        Fdate: String,
        FinDate: String,
        Fzqdm: String,
        Fszsh: String,
        Fjyxwh: String,
        FBS: String,
        Fje: String,
        Fyj: String,
        Fjsf: String,
        Fyhs: String,
        Fzgf: String,
        Fghf: String,
        Fgzlx: String,
        Fhggain: String,
        Ffxj: String,
        Fsssje: String,
        Fzqbz: String,
        Fywbz: String,
        Fqsbz: String,
        Fqtf: String,
        Zqdm: String,
        Fjyfs: String,
        Fsh: String,
        Fzzr: String,
        Fchk: String,
        Fzlh: String,
        Ftzbz: String,
        Fqsghf: String,
        Fgddm: String,
        Fjybz: String,
        Isrtgs: String,
        Fpartid: String,
        Fhtxh: String,
        Fcshtxh: String,
        Frzlv: String,
        Fcsghqx: String,
        Fsjly: String,
        Fbz: String
      )
    })
    save2Mysql(XGSGTempRDD,sparkSession,"CDR_wmz")

  }

  /**
    * 可转债、可交换债预发行；新股预发行，申购确认计算规则
    *
    * @param sparkSession
    * @param SGQRCalculateRDD
    * @param CSGDZHBroadCast
    * @param LSETLISTBroadCast
    */
  def SGQRCalculate(sparkSession: SparkSession, SGQRCalculateRDD: RDD[Row], CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]]): Unit = {
    val SGQRTempRDD = SGQRCalculateRDD.map(row => {
      val Zqzh: String = row.getAs[String]("ZQZH")
      //获取资产代码
      val FSETID: String = getFSETID(Zqzh, CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]])
      val Fdate: String = row.getAs[String]("JSRQ")
      val FinDate: String = getYWDate

      var Fzqdm = ""
      val FzqdmTemp: String = row.getAs[String]("ZQDM1")
      if (FzqdmTemp.startsWith("712")) {
        Fzqdm = replaceString(FzqdmTemp, "609")
      } else if (FzqdmTemp.startsWith("734") || FzqdmTemp.startsWith("732")) {
        Fzqdm = replaceString(FzqdmTemp, "603")
      } else if (FzqdmTemp.startsWith("740") || FzqdmTemp.startsWith("730") || FzqdmTemp.startsWith("731")) {
        Fzqdm = replaceString(FzqdmTemp, "600")
      } else if (FzqdmTemp.startsWith("715")) {
        Fzqdm = replaceString(FzqdmTemp, "605")
      }

      val Fszsh: String = "H"
      val Fjyxwh: String = row.getAs[String]("XWH1")
      val FBS: String = "B"
      val Fje = mul(row.getAs[String]("sl"), row.getAs[String]("Jg1"), 2)
      val Fsl = row.getAs[String]("s1")
      val Fyj = "0"
      val Fjsf = getAbs(row, "JSF")
      val Fyhs = getAbs(row, "YHS")
      val Fzgf = getAbs(row, "ZHF")
      val Fghf = getAbs(row, "GHF")
      val Fgzlx = "0"
      val Fhggain = "0"
      val Ffxj = "0"

      val Fqsbz = "N"
      val Fqtf = "0"
      val Zqdm = row.getAs[String]("ZQDM1")
      val Fjyfs = "PT"
      val Fsh = "1"
      val Fzzr = " "
      val Fchk = " "
      val Fzlh = "0"
      val Ftzbz = " "
      val Fqsghf = "0"
      val Fgddm = Zqzh
      val Fjybz = " "
      val Isrtgs = "1"
      val Fpartid = " "
      val Fhtxh = " "
      val Fcshtxh = " "
      val Frzlv = "0"
      val Fcsghqx = "0"
      val Fsjly = " "
      val Fbz = "RMB"

      val Fsssje = getFsssje(FBS: String, Fje: String, Fjsf: String, Fyhs: String, Fzgf: String, Fghf: String, Fqtf: String, Fqsghf: String) //TODO
      var Fzqbz = getBz("712", Zqdm, "CDRGP", "XG")
      val Fywbz = getBz("712", Zqdm, "CDRGPPSZQQR", "XGPSZQQR")


      SHjsmxHzjkqs(
        FSETID: String,
        Fdate: String,
        FinDate: String,
        Fzqdm: String,
        Fszsh: String,
        Fjyxwh: String,
        FBS: String,
        Fje: String,
        Fyj: String,
        Fjsf: String,
        Fyhs: String,
        Fzgf: String,
        Fghf: String,
        Fgzlx: String,
        Fhggain: String,
        Ffxj: String,
        Fsssje: String,
        Fzqbz: String,
        Fywbz: String,
        Fqsbz: String,
        Fqtf: String,
        Zqdm: String,
        Fjyfs: String,
        Fsh: String,
        Fzzr: String,
        Fchk: String,
        Fzlh: String,
        Ftzbz: String,
        Fqsghf: String,
        Fgddm: String,
        Fjybz: String,
        Isrtgs: String,
        Fpartid: String,
        Fhtxh: String,
        Fcshtxh: String,
        Frzlv: String,
        Fcsghqx: String,
        Fsjly: String,
        Fbz: String
      )
    })
    save2Mysql(SGQRTempRDD,sparkSession,"CDR_wmz")

  }

  /**
    * 可转债、可交换债预发行；新股预发行，新债申购计算规则
    *
    * @param sparkSession
    * @param XZSGCalculateRDD
    * @param CSGDZHBroadCast
    * @param LSETLISTBroadCast
    */
  def XZSGCalculate(sparkSession: SparkSession, XZSGCalculateRDD: RDD[Row], CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]]): Unit = {
    val XZSGTempRDD = XZSGCalculateRDD.map(row => {
      val Zqzh: String = row.getAs[String]("ZQZH")
      //获取资产代码
      val FSETID: String = getFSETID(Zqzh, CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]])
      val Fdate: String = getNextWorkDay(sparkSession, getYWDate)
      val FinDate: String = getYWDate

      var Fzqdm = ""
      val FzqdmTemp: String = row.getAs[String]("ZQDM1")
      if (FzqdmTemp.startsWith("783") || FzqdmTemp.startsWith("754")) {
        Fzqdm = replaceString(FzqdmTemp, "113")
      } else if (FzqdmTemp.startsWith("759")) {
        Fzqdm = replaceString(FzqdmTemp, "132")
      } else {
        Fzqdm = replaceString(FzqdmTemp, "110")
      }

      val Fszsh: String = "H"
      val Fjyxwh: String = row.getAs[String]("XWH1")
      val FBS: String = "B"
      val Fje = ((BigDecimal(row.getAs[String]("CJSL")).abs / 100) * BigDecimal(row.getAs[String]("JG1")).setScale(2, RoundingMode.HALF_UP)).toString()
      val Fsl = (BigDecimal(row.getAs[String]("CJSL")).abs / 100).toString()
      val Fyj = "0"
      val Fjsf = getAbs(row, "JSF")
      val Fyhs = getAbs(row, "YHS")
      val Fzgf = getAbs(row, "ZHF")
      val Fghf = getAbs(row, "GHF")
      val Fgzlx = "0"
      val Fhggain = "0"
      val Ffxj = "0"

      val Fqsbz = "N"
      val Fqtf = "0"
      val Zqdm = row.getAs[String]("ZQDM1")
      val Fjyfs = "PT"
      val Fsh = "1"
      val Fzzr = " "
      val Fchk = " "
      val Fzlh = "0"
      val Ftzbz = " "
      val Fqsghf = "0"
      val Fgddm = Zqzh
      val Fjybz = " "
      val Isrtgs = "1"
      val Fpartid = " "
      val Fhtxh = " "
      val Fcshtxh = " "
      val Frzlv = "0"
      val Fcsghqx = "0"
      val Fsjly = " "
      val Fbz = "RMB"

      val Fsssje = getFsssje(FBS: String, Fje: String, Fjsf: String, Fyhs: String, Fzgf: String, Fghf: String, Fqtf: String, Fqsghf: String) //TODO
      var Fzqbz = "XZ"
      val Fywbz = getBz("759", Zqdm, "KJHGSZQPSZQ", "KZZPSZQ")


      SHjsmxHzjkqs(
        FSETID: String,
        Fdate: String,
        FinDate: String,
        Fzqdm: String,
        Fszsh: String,
        Fjyxwh: String,
        FBS: String,
        Fje: String,
        Fyj: String,
        Fjsf: String,
        Fyhs: String,
        Fzgf: String,
        Fghf: String,
        Fgzlx: String,
        Fhggain: String,
        Ffxj: String,
        Fsssje: String,
        Fzqbz: String,
        Fywbz: String,
        Fqsbz: String,
        Fqtf: String,
        Zqdm: String,
        Fjyfs: String,
        Fsh: String,
        Fzzr: String,
        Fchk: String,
        Fzlh: String,
        Ftzbz: String,
        Fqsghf: String,
        Fgddm: String,
        Fjybz: String,
        Isrtgs: String,
        Fpartid: String,
        Fhtxh: String,
        Fcshtxh: String,
        Frzlv: String,
        Fcsghqx: String,
        Fsjly: String,
        Fbz: String
      )
    })
    save2Mysql(XZSGTempRDD,sparkSession,"CDR_wmz")

  }

  /**
    * 可转债、可交换债预发行；新股预发行，新债申购确认计算规则
    *
    * @param sparkSession
    * @param XZSGQRCalculateRDD
    * @param CSGDZHBroadCast
    * @param LSETLISTBroadCast
    */
  def XZSGQRCalculate(sparkSession: SparkSession, XZSGQRCalculateRDD: RDD[Row], CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]]): Unit = {
    val XZSGQTempRDD = XZSGQRCalculateRDD.map(row => {
      val Zqzh: String = row.getAs[String]("ZQZH")
      //获取资产代码
      val FSETID: String = getFSETID(Zqzh, CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]])
      val Fdate: String = row.getAs[String]("JSRQ")
      val FinDate: String = getYWDate

      var Fzqdm = ""
      val FzqdmTemp: String = row.getAs[String]("ZQDM1")
      if (FzqdmTemp.startsWith("783") || FzqdmTemp.startsWith("754")) {
        Fzqdm = replaceString(FzqdmTemp, "113")
      } else if (FzqdmTemp.startsWith("759")) {
        Fzqdm = replaceString(FzqdmTemp, "132")
      } else {
        Fzqdm = replaceString(FzqdmTemp, "110")
      }

      val Fszsh: String = "H"
      val Fjyxwh: String = row.getAs[String]("XWH1")
      val FBS: String = "B"
      val Fje = ((BigDecimal(row.getAs[String]("CJSL")).abs / 100) * BigDecimal(row.getAs[String]("JG1")).setScale(2, RoundingMode.HALF_UP)).toString()
      val Fsl = (BigDecimal(row.getAs[String]("CJSL")).abs / 100).toString()
      val Fyj = "0"
      val Fjsf = getAbs(row, "JSF")
      val Fyhs = getAbs(row, "YHS")
      val Fzgf = getAbs(row, "ZHF")
      val Fghf = getAbs(row, "GHF")
      val Fgzlx = "0"
      val Fhggain = "0"
      val Ffxj = "0"

      val Fqsbz = "N"
      val Fqtf = "0"
      val Zqdm = row.getAs[String]("ZQDM1")
      val Fjyfs = "PT"
      val Fsh = "1"
      val Fzzr = " "
      val Fchk = " "
      val Fzlh = "0"
      val Ftzbz = " "
      val Fqsghf = "0"
      val Fgddm = Zqzh
      val Fjybz = " "
      val Isrtgs = "1"
      val Fpartid = " "
      val Fhtxh = " "
      val Fcshtxh = " "
      val Frzlv = "0"
      val Fcsghqx = "0"
      val Fsjly = " "
      val Fbz = "RMB"

      val Fsssje = getFsssje(FBS: String, Fje: String, Fjsf: String, Fyhs: String, Fzgf: String, Fghf: String, Fqtf: String, Fqsghf: String) //TODO
      var Fzqbz = "XZ"
      val Fywbz = getBz("759", Zqdm, "KJHGSZQPSZQQR", "KZZPSZQQR")


      SHjsmxHzjkqs(
        FSETID: String,
        Fdate: String,
        FinDate: String,
        Fzqdm: String,
        Fszsh: String,
        Fjyxwh: String,
        FBS: String,
        Fje: String,
        Fyj: String,
        Fjsf: String,
        Fyhs: String,
        Fzgf: String,
        Fghf: String,
        Fgzlx: String,
        Fhggain: String,
        Ffxj: String,
        Fsssje: String,
        Fzqbz: String,
        Fywbz: String,
        Fqsbz: String,
        Fqtf: String,
        Zqdm: String,
        Fjyfs: String,
        Fsh: String,
        Fzzr: String,
        Fchk: String,
        Fzlh: String,
        Ftzbz: String,
        Fqsghf: String,
        Fgddm: String,
        Fjybz: String,
        Isrtgs: String,
        Fpartid: String,
        Fhtxh: String,
        Fcshtxh: String,
        Frzlv: String,
        Fcsghqx: String,
        Fsjly: String,
        Fbz: String
      )
    })
    save2Mysql(XZSGQTempRDD,sparkSession,"CDR_wmz")

  }

  /**
    * 老股东配股配债-配股
    *
    * @param sparkSession
    * @param PGCalculateRDD
    * @param CSGDZHBroadCast
    * @param LSETLISTBroadCast
    */
  def PGCalculate(sparkSession: SparkSession, PGCalculateRDD: RDD[Row], CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]]): Unit = {
    val PGTempRDD = PGCalculateRDD.map(row => {
      val Zqzh: String = row.getAs[String]("ZQZH")
      //获取资产代码
      val FSETID: String = getFSETID(Zqzh, CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]])
      val Fdate: String = getYWDate
      val FinDate: String = getYWDate

      var Fzqdm = ""
      val FzqdmTemp: String = row.getAs[String]("ZQDM1")
      if (FzqdmTemp.startsWith("70")) {
        Fzqdm = replaceString(FzqdmTemp, "600")
      } else if (FzqdmTemp.startsWith("714")) {
        Fzqdm = replaceString(FzqdmTemp, "609")
      } else {
        Fzqdm = replaceString(FzqdmTemp, "601")
      }

      val Fszsh: String = "H"
      val Fjyxwh: String = row.getAs[String]("XWH1")
      val FBS: String = row.getAs[String]("MMBZ")
      val Fje = BigDecimal(row.getAs[String]("CJSL")).abs.toString()
      val Fsl = BigDecimal(row.getAs[String]("CJSL")).abs.toString()
      val Fyj = "0"
      val Fjsf = getAbs(row, "JSF")
      val Fyhs = getAbs(row, "YHS")
      val Fzgf = getAbs(row, "ZHF")
      val Fghf = getAbs(row, "GHF")
      val Fgzlx = "0"
      val Fhggain = "0"
      val Ffxj = "0"

      val Fqsbz = "N"
      val Fqtf = "0"
      val Zqdm = row.getAs[String]("ZQDM1")
      val Fjyfs = "PT"
      val Fsh = "1"
      val Fzzr = " "
      val Fchk = " "
      val Fzlh = "0"
      val Ftzbz = " "
      val Fqsghf = "0"
      val Fgddm = Zqzh
      val Fjybz = " "
      val Isrtgs = "1"
      val Fpartid = " "
      val Fhtxh = " "
      val Fcshtxh = " "
      val Frzlv = "0"
      val Fcsghqx = "0"
      val Fsjly = " "
      val Fbz = "RMB"

      val Fsssje = getFsssje(FBS: String, Fje: String, Fjsf: String, Fyhs: String, Fzgf: String, Fghf: String, Fqtf: String, Fqsghf: String) //TODO
      var Fzqbz = "QY"
      val Fywbz = "PG"


      SHjsmxHzjkqs(
        FSETID: String,
        Fdate: String,
        FinDate: String,
        Fzqdm: String,
        Fszsh: String,
        Fjyxwh: String,
        FBS: String,
        Fje: String,
        Fyj: String,
        Fjsf: String,
        Fyhs: String,
        Fzgf: String,
        Fghf: String,
        Fgzlx: String,
        Fhggain: String,
        Ffxj: String,
        Fsssje: String,
        Fzqbz: String,
        Fywbz: String,
        Fqsbz: String,
        Fqtf: String,
        Zqdm: String,
        Fjyfs: String,
        Fsh: String,
        Fzzr: String,
        Fchk: String,
        Fzlh: String,
        Ftzbz: String,
        Fqsghf: String,
        Fgddm: String,
        Fjybz: String,
        Isrtgs: String,
        Fpartid: String,
        Fhtxh: String,
        Fcshtxh: String,
        Frzlv: String,
        Fcsghqx: String,
        Fsjly: String,
        Fbz: String
      )
    })
    save2Mysql(PGTempRDD,sparkSession,"CDR_wmz")

  }

  /**
    * 老股东配股配债-配债
    *
    * @param sparkSession
    * @param PZCalculateRDD
    * @param CSGDZHBroadCast
    * @param LSETLISTBroadCast
    */
  def PZCalculate(sparkSession: SparkSession, PZCalculateRDD: RDD[Row], CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]]): Unit = {
    val PZTempRDD = PZCalculateRDD.map(row => {
      val Zqzh: String = row.getAs[String]("ZQZH")
      //获取资产代码
      val FSETID: String = getFSETID(Zqzh, CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]])
      val Fdate: String = getYWDate
      val FinDate: String = getYWDate

      var Fzqdm = ""
      if (row.getAs[String]("ZQDM2") == null) {
        val FzqdmTemp = row.getAs[String]("ZQDM1")
        if (FzqdmTemp.startsWith("704")) {
          Fzqdm = replaceString(FzqdmTemp, "110")
        } else {
          Fzqdm = replaceString(FzqdmTemp, "113")
        }
      } else {
        Fzqdm = row.getAs[String]("ZQDM2")
      }


      val Fszsh: String = "H"
      val Fjyxwh: String = row.getAs[String]("XWH1")
      val FBS: String = row.getAs[String]("MMBZ")
      val Fje = BigDecimal(row.getAs[String]("CJSL")).abs.toString()
      val Fsl = (BigDecimal(row.getAs[String]("CJSL")).abs * 100).toString()
      val Fyj = "0"
      val Fjsf = getAbs(row, "JSF")
      val Fyhs = getAbs(row, "YHS")
      val Fzgf = getAbs(row, "ZHF")
      val Fghf = getAbs(row, "GHF")
      val Fgzlx = "0"
      val Fhggain = "0"
      val Ffxj = "0"

      val Fqsbz = "N"
      val Fqtf = "0"
      val Zqdm = row.getAs[String]("ZQDM1")
      val Fjyfs = "PT"
      val Fsh = "1"
      val Fzzr = " "
      val Fchk = " "
      val Fzlh = "0"
      val Ftzbz = " "
      val Fqsghf = "0"
      val Fgddm = Zqzh
      val Fjybz = " "
      val Isrtgs = "1"
      val Fpartid = " "
      val Fhtxh = " "
      val Fcshtxh = " "
      val Frzlv = "0"
      val Fcsghqx = "0"
      val Fsjly = " "
      val Fbz = "RMB"

      val Fsssje = getFsssje(FBS: String, Fje: String, Fjsf: String, Fyhs: String, Fzgf: String, Fghf: String, Fqtf: String, Fqsghf: String) //TODO
      var Fzqbz = "XZ"
      val Fywbz = "KZZXZ"


      SHjsmxHzjkqs(
        FSETID: String,
        Fdate: String,
        FinDate: String,
        Fzqdm: String,
        Fszsh: String,
        Fjyxwh: String,
        FBS: String,
        Fje: String,
        Fyj: String,
        Fjsf: String,
        Fyhs: String,
        Fzgf: String,
        Fghf: String,
        Fgzlx: String,
        Fhggain: String,
        Ffxj: String,
        Fsssje: String,
        Fzqbz: String,
        Fywbz: String,
        Fqsbz: String,
        Fqtf: String,
        Zqdm: String,
        Fjyfs: String,
        Fsh: String,
        Fzzr: String,
        Fchk: String,
        Fzlh: String,
        Ftzbz: String,
        Fqsghf: String,
        Fgddm: String,
        Fjybz: String,
        Isrtgs: String,
        Fpartid: String,
        Fhtxh: String,
        Fcshtxh: String,
        Frzlv: String,
        Fcsghqx: String,
        Fsjly: String,
        Fbz: String
      )
    })
    save2Mysql(PZTempRDD,sparkSession,"CDR_wmz")

  }

  /**
    * 债转股、债换股-债转股债券项计算规则
    *
    * @param sparkSession
    * @param ZZGZQCalculateRDD
    * @param CSGDZHBroadCast
    * @param LSETLISTBroadCast
    */
  def ZZGZQCalculate(sparkSession: SparkSession, ZZGZQCalculateRDD: RDD[Row], CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]]): Unit = {
    val ZZGZQTempRDD = ZZGZQCalculateRDD.map(row => {
      val Zqzh: String = row.getAs[String]("ZQZH")
      //获取资产代码
      val FSETID: String = getFSETID(Zqzh, CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]])
      val Fdate: String = getYWDate
      val FinDate: String = getNextWorkDay(sparkSession, getYWDate)

      var Fzqdm = row.getAs[String]("ZQDM1")
      val Fszsh: String = "H"
      val Fjyxwh: String = row.getAs[String]("XWH1")
      val FBS: String = row.getAs[String]("MMBZ")
      val Fje = row.getAs[String]("QSJE")
      val Fsl = (BigDecimal(row.getAs[String]("SL")).abs / 100).toString()
      val Fyj = "0"
      val Fjsf = getAbs(row, "JSF")
      val Fyhs = getAbs(row, "YHS")
      val Fzgf = getAbs(row, "ZHF")
      val Fghf = getAbs(row, "GHF")
      val Fgzlx = "0"
      val Fhggain = "0"
      val Ffxj = "0"

      val Fqsbz = "N"
      val Fqtf = "0"
      val Zqdm = row.getAs[String]("ZQDM2")
      val Fjyfs = "PT"
      val Fsh = "1"
      val Fzzr = " "
      val Fchk = " "
      val Fzlh = "0"
      val Ftzbz = " "
      val Fqsghf = "0"
      val Fgddm = Zqzh
      val Fjybz = " "
      val Isrtgs = "1"
      val Fpartid = " "
      val Fhtxh = " "
      val Fcshtxh = " "
      val Frzlv = "0"
      val Fcsghqx = "0"
      val Fsjly = " "
      val Fbz = "RMB"

      val Fsssje = getFsssje(FBS: String, Fje: String, Fjsf: String, Fyhs: String, Fzgf: String, Fghf: String, Fqtf: String, Fqsghf: String) //TODO
      var Fzqbz = "ZQ"
      val Fywbz = "KZZGP"


      SHjsmxHzjkqs(
        FSETID: String,
        Fdate: String,
        FinDate: String,
        Fzqdm: String,
        Fszsh: String,
        Fjyxwh: String,
        FBS: String,
        Fje: String,
        Fyj: String,
        Fjsf: String,
        Fyhs: String,
        Fzgf: String,
        Fghf: String,
        Fgzlx: String,
        Fhggain: String,
        Ffxj: String,
        Fsssje: String,
        Fzqbz: String,
        Fywbz: String,
        Fqsbz: String,
        Fqtf: String,
        Zqdm: String,
        Fjyfs: String,
        Fsh: String,
        Fzzr: String,
        Fchk: String,
        Fzlh: String,
        Ftzbz: String,
        Fqsghf: String,
        Fgddm: String,
        Fjybz: String,
        Isrtgs: String,
        Fpartid: String,
        Fhtxh: String,
        Fcshtxh: String,
        Frzlv: String,
        Fcsghqx: String,
        Fsjly: String,
        Fbz: String
      )
    })
    save2Mysql(ZZGZQTempRDD,sparkSession,"CDR_wmz")

  }


  /**
    * 债转股、债换股-债转股股票项计算规则
    *
    * @param sparkSession
    * @param ZZGGPCalculateRDD
    * @param CSGDZHBroadCast
    * @param LSETLISTBroadCast
    * @param QSJEBroadcast
    */
  def ZZGGPCalculate(sparkSession: SparkSession, ZZGGPCalculateRDD: RDD[Row], CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]], QSJEBroadcast: Broadcast[Array[Row]]): Unit = {
    val ZZGGPTempRDD = ZZGGPCalculateRDD.map(row => {

      //TODO .toString方法可能存在问题，数据格式可能错误。不能确定是否是只有一条数据

      val QSJE: String = QSJEBroadcast.value.filter(lines => {
        row.getAs[String]("CJBH").equals(lines(8))
      })(36).toString


      val Zqzh: String = row.getAs[String]("ZQZH")
      //获取资产代码
      val FSETID: String = getFSETID(Zqzh, CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]])
      val Fdate: String = getYWDate
      val FinDate: String = getNextWorkDay(sparkSession, getYWDate)

      var Fzqdm = row.getAs[String]("ZQDM1")
      val Fszsh: String = "H"
      val Fjyxwh: String = row.getAs[String]("XWH1")
      val FBS: String = row.getAs[String]("MMBZ")
      val Fje = QSJE
      val Fsl = row.getAs[String]("SL")
      val Fyj = "0"
      val Fjsf = getAbs(row, "JSF")
      val Fyhs = getAbs(row, "YHS")
      val Fzgf = getAbs(row, "ZHF")
      val Fghf = getAbs(row, "GHF")
      val Fgzlx = "0"
      val Fhggain = "0"
      val Ffxj = "0"

      val Fqsbz = "N"
      val Fqtf = "0"
      val Zqdm = row.getAs[String]("ZQDM2")
      val Fjyfs = "PT"
      val Fsh = "1"
      val Fzzr = " "
      val Fchk = " "
      val Fzlh = "0"
      val Ftzbz = " "
      val Fqsghf = "0"
      val Fgddm = Zqzh
      val Fjybz = " "
      val Isrtgs = "1"
      val Fpartid = " "
      val Fhtxh = " "
      val Fcshtxh = " "
      val Frzlv = "0"
      val Fcsghqx = "0"
      val Fsjly = " "
      val Fbz = "RMB"

      val Fsssje = getFsssje(FBS: String, Fje: String, Fjsf: String, Fyhs: String, Fzgf: String, Fghf: String, Fqtf: String, Fqsghf: String) //TODO
      var Fzqbz = "GP"
      val Fywbz = "KZZGP"


      SHjsmxHzjkqs(
        FSETID: String,
        Fdate: String,
        FinDate: String,
        Fzqdm: String,
        Fszsh: String,
        Fjyxwh: String,
        FBS: String,
        Fje: String,
        Fyj: String,
        Fjsf: String,
        Fyhs: String,
        Fzgf: String,
        Fghf: String,
        Fgzlx: String,
        Fhggain: String,
        Ffxj: String,
        Fsssje: String,
        Fzqbz: String,
        Fywbz: String,
        Fqsbz: String,
        Fqtf: String,
        Zqdm: String,
        Fjyfs: String,
        Fsh: String,
        Fzzr: String,
        Fchk: String,
        Fzlh: String,
        Ftzbz: String,
        Fqsghf: String,
        Fgddm: String,
        Fjybz: String,
        Isrtgs: String,
        Fpartid: String,
        Fhtxh: String,
        Fcshtxh: String,
        Frzlv: String,
        Fcsghqx: String,
        Fsjly: String,
        Fbz: String
      )
    })
    save2Mysql(ZZGGPTempRDD,sparkSession,"CDR_wmz")

  }

  /**
    * 债转股、债换股-债转股债券项2计算规则
    *
    * @param sparkSession
    * @param ZZGZQ2CalculateRDD
    * @param CSGDZHBroadCast
    * @param LSETLISTBroadCast
    */
  def ZZGZQ2Calculate(sparkSession: SparkSession, ZZGZQ2CalculateRDD: RDD[Row], CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]]): Unit = {
   val ZZGZQ2TempRDD = ZZGZQ2CalculateRDD.map(row => {
      val Zqzh: String = row.getAs[String]("ZQZH")
      //获取资产代码
      val FSETID: String = getFSETID(Zqzh, CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]])
      val Fdate: String = getYWDate
      val FinDate: String = getNextWorkDay(sparkSession, getYWDate)

      var Fzqdm = row.getAs[String]("ZQDM1")
      val Fszsh: String = "H"
      val Fjyxwh: String = row.getAs[String]("XWH1")
      val FBS: String = row.getAs[String]("MMBZ")
      val Fje = row.getAs[String]("QSJE")
      val Fsl = (BigDecimal(row.getAs[String]("SL")).abs / 100).toString()
      val Fyj = "0"
      val Fjsf = getAbs(row, "JSF")
      val Fyhs = getAbs(row, "YHS")
      val Fzgf = getAbs(row, "ZHF")
      val Fghf = getAbs(row, "GHF")
      val Fgzlx = "0"
      val Fhggain = "0"
      val Ffxj = "0"

      val Fqsbz = "N"
      val Fqtf = "0"
      val Zqdm = row.getAs[String]("ZQDM2")
      val Fjyfs = "PT"
      val Fsh = "1"
      val Fzzr = " "
      val Fchk = " "
      val Fzlh = "0"
      val Ftzbz = " "
      val Fqsghf = "0"
      val Fgddm = Zqzh
      val Fjybz = " "
      val Isrtgs = "1"
      val Fpartid = " "
      val Fhtxh = " "
      val Fcshtxh = " "
      val Frzlv = "0"
      val Fcsghqx = "0"
      val Fsjly = " "
      val Fbz = "RMB"

      val Fsssje = getFsssje(FBS: String, Fje: String, Fjsf: String, Fyhs: String, Fzgf: String, Fghf: String, Fqtf: String, Fqsghf: String) //TODO
      var Fzqbz = "ZQ"
      var Fywbz = ""
      if (row.getAs[String]("ZQDM2").startsWith("138")) {
        Fywbz = "KJHSMZQGP"
      } else {
        Fywbz = "KJHGSZQGP"
      }


      SHjsmxHzjkqs(
        FSETID: String,
        Fdate: String,
        FinDate: String,
        Fzqdm: String,
        Fszsh: String,
        Fjyxwh: String,
        FBS: String,
        Fje: String,
        Fyj: String,
        Fjsf: String,
        Fyhs: String,
        Fzgf: String,
        Fghf: String,
        Fgzlx: String,
        Fhggain: String,
        Ffxj: String,
        Fsssje: String,
        Fzqbz: String,
        Fywbz: String,
        Fqsbz: String,
        Fqtf: String,
        Zqdm: String,
        Fjyfs: String,
        Fsh: String,
        Fzzr: String,
        Fchk: String,
        Fzlh: String,
        Ftzbz: String,
        Fqsghf: String,
        Fgddm: String,
        Fjybz: String,
        Isrtgs: String,
        Fpartid: String,
        Fhtxh: String,
        Fcshtxh: String,
        Frzlv: String,
        Fcsghqx: String,
        Fsjly: String,
        Fbz: String
      )
    })
    save2Mysql(ZZGZQ2TempRDD,sparkSession,"CDR_wmz")

  }

  /**
    * 债转股、债换股-债转股股票项2计算规则
    *
    * @param sparkSession
    * @param ZZGZQ2CalculateRDD
    * @param CSGDZHBroadCast
    * @param LSETLISTBroadCast
    */
  def ZZGGP2Calculate(sparkSession: SparkSession, ZZGGP2CalculateRDD: RDD[Row], CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]]): Unit = {
    val ZZGGP2TempRDD = ZZGGP2CalculateRDD.map(row => {
      val Zqzh: String = row.getAs[String]("ZQZH")
      //获取资产代码
      val FSETID: String = getFSETID(Zqzh, CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]])
      val Fdate: String = getYWDate
      val FinDate: String = getNextWorkDay(sparkSession, getYWDate)

      var Fzqdm = row.getAs[String]("ZQDM1")
      val Fszsh: String = "H"
      val Fjyxwh: String = row.getAs[String]("XWH1")
      val FBS: String = row.getAs[String]("MMBZ")
      val Fje = row.getAs[String]("QSJE")
      val Fsl = (BigDecimal(row.getAs[String]("SL")).abs / 100).toString()
      val Fyj = "0"
      val Fjsf = getAbs(row, "JSF")
      val Fyhs = getAbs(row, "YHS")
      val Fzgf = getAbs(row, "ZHF")
      val Fghf = getAbs(row, "GHF")
      val Fgzlx = "0"
      val Fhggain = "0"
      val Ffxj = "0"

      val Fqsbz = "N"
      val Fqtf = "0"
      val Zqdm = row.getAs[String]("ZQDM2")
      val Fjyfs = "PT"
      val Fsh = "1"
      val Fzzr = " "
      val Fchk = " "
      val Fzlh = "0"
      val Ftzbz = " "
      val Fqsghf = "0"
      val Fgddm = Zqzh
      val Fjybz = " "
      val Isrtgs = "1"
      val Fpartid = " "
      val Fhtxh = " "
      val Fcshtxh = " "
      val Frzlv = "0"
      val Fcsghqx = "0"
      val Fsjly = " "
      val Fbz = "RMB"

      val Fsssje = getFsssje(FBS: String, Fje: String, Fjsf: String, Fyhs: String, Fzgf: String, Fghf: String, Fqtf: String, Fqsghf: String) //TODO
      var Fzqbz = "GP"
      var Fywbz = ""
      if (row.getAs[String]("ZQDM2").startsWith("138")) {
        Fywbz = "KJHSMZQGP"
      } else {
        Fywbz = "KJHGSZQGP"
      }


      SHjsmxHzjkqs(
        FSETID: String,
        Fdate: String,
        FinDate: String,
        Fzqdm: String,
        Fszsh: String,
        Fjyxwh: String,
        FBS: String,
        Fje: String,
        Fyj: String,
        Fjsf: String,
        Fyhs: String,
        Fzgf: String,
        Fghf: String,
        Fgzlx: String,
        Fhggain: String,
        Ffxj: String,
        Fsssje: String,
        Fzqbz: String,
        Fywbz: String,
        Fqsbz: String,
        Fqtf: String,
        Zqdm: String,
        Fjyfs: String,
        Fsh: String,
        Fzzr: String,
        Fchk: String,
        Fzlh: String,
        Ftzbz: String,
        Fqsghf: String,
        Fgddm: String,
        Fjybz: String,
        Isrtgs: String,
        Fpartid: String,
        Fhtxh: String,
        Fcshtxh: String,
        Frzlv: String,
        Fcsghqx: String,
        Fsjly: String,
        Fbz: String
      )
    })
    save2Mysql(ZZGGP2TempRDD,sparkSession,"CDR_wmz")

  }

  /**
    * 要约收购、回售-要约收购计算规则
    *
    * @param sparkSession
    * @param YYSGCalculateRDD
    * @param CSGDZHBroadCast
    * @param LSETLISTBroadCast
    * @param LVARLISTBroadCast
    * @param CSQSFYLVBroadcast
    */
  def YYSGCalculate(sparkSession: SparkSession, YYSGCalculateRDD: RDD[Row], CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]], LVARLISTBroadCast: Broadcast[Array[String]], CSQSFYLVBroadcast: Broadcast[Array[String]], CSSYSYJLVBroadCast: Broadcast[Array[String]]): Unit = {
    val YYSGTempRDD = YYSGCalculateRDD.map(row => {

      var Fzqdm = row.getAs[String]("ZQDM1")

      val FSETID: String = getFSETID(row.getAs[String]("ZQZH"), CSGDZHBroadCast, LSETLISTBroadCast)
      val GPMZArr: Array[String] = getLVARLISTValue(FSETID, "股票面值" + Fzqdm, LVARLISTBroadCast)
      val GPMZ = GPMZArr(1)

      val FSETID2: String = getFSETID(row.getAs[String]("ZQZH"), CSGDZHBroadCast, LSETLISTBroadCast)
      val bqsghfghfArr: Array[String] = getLVARLISTValue(FSETID2, "计算券商过户费减去过户费" + Fzqdm, LVARLISTBroadCast)
      val bqsghfghfValue = bqsghfghfArr(1)
      var bqsghfghf = false
      if (bqsghfghfValue == 1) {
        bqsghfghf = true
      }


      val Fzqbz = "GP"
      val Fszsh: String = "H"

      val LvArr: Array[String] = CSQSFYLVBroadcast.value.filter(lines => {
        lines(2).equals(Fzqbz) &&
          lines(3).equals(Fszsh)
      })
      val FLV = BigDecimal(LvArr(8)) //券商过户费率
      val FLVZK = BigDecimal(LvArr(10)) //折扣率
      val FFYMIN = BigDecimal(LvArr(9)) //最小值

      val Fywbz = "YYSell"

      val Zqzh: String = row.getAs[String]("ZQZH")
      //获取资产代码
      val Fdate: String = getYWDate
      val FinDate: String = getYWDate


      val Fjyxwh: String = row.getAs[String]("XWH1")
      val FBS: String = row.getAs[String]("MMBZ")

      val Fsl = BigDecimal(row.getAs[String]("SL")).abs.toString()

      val Fjsf = getAbs(row, "JSF")
      val Fyhs = getAbs(row, "YHS")
      val Fzgf = getAbs(row, "ZHF")
      val Fghf = getAbs(row, "GHF")
      val Fgzlx = "0"
      val Fhggain = "0"
      val Ffxj = "0"

      val Fqsbz = "N"
      val Fqtf = "0"
      val Zqdm = row.getAs[String]("ZQDM1")
      val Fjyfs = "PT"
      val Fsh = "1"
      val Fzzr = " "
      val Fchk = " "
      val Fzlh = "0"
      val Ftzbz = " "

      //券商过户费计算规则
      var FqsghfValue = ""
      if (bqsghfghf == true) {
        FqsghfValue = (BigDecimal(row.getAs[String]("CJSL")) * BigDecimal(GPMZ) * FLV * FLVZK - BigDecimal(Fghf)).setScale(2, RoundingMode.HALF_UP).toString()
      } else {
        FqsghfValue = (BigDecimal(row.getAs[String]("CJSL")) * BigDecimal(GPMZ) * FLV * FLVZK).setScale(2, RoundingMode.HALF_UP).toString()
      }
      var Fqsghf = ""
      if (BigDecimal(FqsghfValue) < FFYMIN) {
        Fqsghf = FFYMIN.toString()
      } else {
        Fqsghf = FqsghfValue
      }


      // 佣金计算规则
      val Fje = row.getAs[String]("QSJE")
      var Fyj = ""
      val YJLV: String = CSSYSYJLVBroadCast.value.filter(lines => {
        lines(3).equals(Fywbz)
      })(5) // 佣金利率

      val QSCDFSFY: String = CSQSFYLVBroadcast.value.filter(lines => {
        lines(2).equals("GP") &&
          lines(4).equals("QSGHD")
      })(5)

      if (QSCDFSFY == 0) {
        Fyj = (BigDecimal(Fje) * BigDecimal(YJLV) - BigDecimal(Fqsghf) - BigDecimal(Fghf)).toString()
      } else {
        Fyj = (BigDecimal(Fje) * BigDecimal(YJLV)).toString()
      }


      val Fgddm = Zqzh
      val Fjybz = " "
      val Isrtgs = "1"
      val Fpartid = " "
      val Fhtxh = " "
      val Fcshtxh = " "
      val Frzlv = "0"
      val Fcsghqx = "0"
      val Fsjly = " "
      val Fbz = "RMB"

      val Fsssje = getFsssje(FBS: String, Fje: String, Fjsf: String, Fyhs: String, Fzgf: String, Fghf: String, Fqtf: String, Fqsghf: String) //TODO


      SHjsmxHzjkqs(
        FSETID: String,
        Fdate: String,
        FinDate: String,
        Fzqdm: String,
        Fszsh: String,
        Fjyxwh: String,
        FBS: String,
        Fje: String,
        Fyj: String,
        Fjsf: String,
        Fyhs: String,
        Fzgf: String,
        Fghf: String,
        Fgzlx: String,
        Fhggain: String,
        Ffxj: String,
        Fsssje: String,
        Fzqbz: String,
        Fywbz: String,
        Fqsbz: String,
        Fqtf: String,
        Zqdm: String,
        Fjyfs: String,
        Fsh: String,
        Fzzr: String,
        Fchk: String,
        Fzlh: String,
        Ftzbz: String,
        Fqsghf: String,
        Fgddm: String,
        Fjybz: String,
        Isrtgs: String,
        Fpartid: String,
        Fhtxh: String,
        Fcshtxh: String,
        Frzlv: String,
        Fcsghqx: String,
        Fsjly: String,
        Fbz: String
      )
    })
    save2Mysql(YYSGTempRDD,sparkSession,"CDR_wmz")

  }

  /**
    * 要约收购、回售-回售计算规则
    *
    * @param sparkSession
    * @param HSCalculateRDD
    * @param CSGDZHBroadCast
    * @param LSETLISTBroadCast
    * @param CSZQXXBroadCast
    * @param LVARLISTBroadCast
    */
  def HSCalculate(sparkSession: SparkSession, HSCalculateRDD: RDD[Row], CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]], CSZQXXBroadCast: Broadcast[Array[String]], LVARLISTBroadCast: Broadcast[Array[String]]): Unit = {
    val HSTempRDD = HSCalculateRDD.map(row => {
      val Zqzh: String = row.getAs[String]("ZQZH")
      //获取资产代码
      val FSETID: String = getFSETID(Zqzh, CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]])
      val Fdate: String = getYWDate
      val FinDate: String = getNextWorkDay(sparkSession, getYWDate)

      var Fzqdm = row.getAs[String]("ZQDM1")
      val Fszsh: String = "H"
      val Fjyxwh: String = row.getAs[String]("XWH1")
      val FBS: String = row.getAs[String]("MMBZ")
      val Fje = row.getAs[String]("QSJE")
      val Fsl = (BigDecimal(row.getAs[String]("SL")).abs / 100).toString()
      val Fyj = "0"
      val Fjsf = getAbs(row, "JSF")
      val Fyhs = getAbs(row, "YHS")
      val Fzgf = getAbs(row, "ZHF")
      val Fghf = getAbs(row, "GHF")
      val Fgzlx = "0"
      val Fhggain = "0"
      val Ffxj = "0"

      val Fqsbz = "N"
      val Fqtf = "0"
      val Zqdm = row.getAs[String]("ZQDM1")
      val Fjyfs = "PT"
      val Fsh = "1"
      val Fzzr = " "
      val Fchk = " "
      val Fzlh = "0"
      val Ftzbz = " "
      val Fqsghf = "0"
      val Fgddm = Zqzh
      val Fjybz = " "
      val Isrtgs = "1"
      val Fpartid = " "
      val Fhtxh = " "
      val Fcshtxh = " "
      val Frzlv = "0"
      val Fcsghqx = "0"
      val Fsjly = " "
      val Fbz = "RMB"

      val Fsssje = getFsssje(FBS: String, Fje: String, Fjsf: String, Fyhs: String, Fzgf: String, Fghf: String, Fqtf: String, Fqsghf: String) //TODO
      var Fzqbz = "ZQ"

      val FZQLB: String = CSZQXXBroadCast.value.filter(lines => {
        lines(0).equals(Zqdm)
      })(11)

      // FYWBZ计算逻辑
      val checkBondInfoValue: Array[String] = getLVARLISTValue(FSETID, "债券类型取债券品种信息维护的债券类型", LVARLISTBroadCast)
      val checkBondInfo = checkBondInfoValue(1)

      val bondEnableDateValue: Array[String] = getLVARLISTValue(FSETID, "债券类型取债券品种信息维护的债券类型启用日期", LVARLISTBroadCast)
      val bondEnableDate = bondEnableDateValue(5)

      val codeSnippetsValue: Array[String] = getLVARLISTValue(FSETID, "债券类型取债券品种信息维护的债券类型代码段", LVARLISTBroadCast)
      val codeSnippets = codeSnippetsValue(1)

      var Fywbz = ""

      if (checkBondInfo == 1 &&
        DateUtils.formattedDate2Long(bondEnableDate, DateUtils.YYYYMMDD).compareTo(DateUtils.formattedDate2Long(getYWDate, DateUtils.YYYYMMDD)) <= 0 &&
        Zqdm.startsWith(codeSnippets)) {

        FZQLB match {
          case "可交换债券" => Fywbz = "KZZHS"
          case "可交换公司债券" => Fywbz = "KJHGSZQHS"
          case "资产证券" => Fywbz = "ZCZQHS"
          case "企业债券" => Fywbz = "QYZQHS"
          case "私募债券" => Fywbz = "SMZQHS"
        }
      } else {
        if (Zqdm.startsWith("10") || Zqdm.startsWith("11")) {
          Fywbz = "KZZHS"
        } else if (Zqdm.startsWith("121")) {
          Fywbz = "ZCZQHS"
        } else if (Zqdm.startsWith("12") && !Zqdm.startsWith("121")) {
          Fywbz = "QYZQHS"
        } else if (Zqdm.startsWith("132")) {
          Fywbz = "KJHGSZQHS"
        }
      }

      SHjsmxHzjkqs(
        FSETID: String,
        Fdate: String,
        FinDate: String,
        Fzqdm: String,
        Fszsh: String,
        Fjyxwh: String,
        FBS: String,
        Fje: String,
        Fyj: String,
        Fjsf: String,
        Fyhs: String,
        Fzgf: String,
        Fghf: String,
        Fgzlx: String,
        Fhggain: String,
        Ffxj: String,
        Fsssje: String,
        Fzqbz: String,
        Fywbz: String,
        Fqsbz: String,
        Fqtf: String,
        Zqdm: String,
        Fjyfs: String,
        Fsh: String,
        Fzzr: String,
        Fchk: String,
        Fzlh: String,
        Ftzbz: String,
        Fqsghf: String,
        Fgddm: String,
        Fjybz: String,
        Isrtgs: String,
        Fpartid: String,
        Fhtxh: String,
        Fcshtxh: String,
        Frzlv: String,
        Fcsghqx: String,
        Fsjly: String,
        Fbz: String
      )
    })
    save2Mysql(HSTempRDD,sparkSession,"CDR_wmz")

  }

  /**
    * 回购-固定收益平台隔夜回购计算规则
    *
    * @param sparkSession
    * @param GDSYGYHGCalculateRDD
    * @param CSGDZHBroadCast
    * @param LSETLISTBroadCast
    */
  def GDSYGYHGCalculate(sparkSession: SparkSession, GDSYGYHGCalculateRDD: RDD[Row], CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]]): Unit = {
    val GDSYGYHGTempRDD = GDSYGYHGCalculateRDD.map(row => {
      val Zqzh: String = row.getAs[String]("ZQZH")
      //获取资产代码
      val FSETID: String = getFSETID(Zqzh, CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]])
      val Fdate: String = getYWDate
      val FinDate: String = getYWDate

      var Fzqdm = row.getAs[String]("ZQDM1")
      val Fszsh: String = "G"
      val Fjyxwh: String = row.getAs[String]("XWH1")
      val FBS: String = row.getAs[String]("MMBZ")
      val Fje = row.getAs[String]("QSJE")
      val Fsl = BigDecimal(row.getAs[String]("SL")).abs.toString()
      val Fyj = "0"
      val Fjsf = getAbs(row, "JSF")
      val Fyhs = getAbs(row, "YHS")
      val Fzgf = getAbs(row, "ZHF")
      val Fghf = getAbs(row, "GHF")
      val Fgzlx = "0"
      val Fhggain = "0"
      val Ffxj = "0"

      val Fqsbz = "N"
      val Fqtf = "0"
      val Zqdm = row.getAs[String]("ZQDM1")
      val Fjyfs = "PT"
      val Fsh = "1"
      val Fzzr = " "
      val Fchk = " "
      val Fzlh = "0"
      val Ftzbz = " "
      val Fqsghf = "0"
      val Fgddm = Zqzh
      val Fjybz = " "
      val Isrtgs = "1"
      val Fpartid = " "
      val Fhtxh = " "
      val Fcshtxh = " "
      val Frzlv = "0"
      val Fcsghqx = "0"
      val Fsjly = " "
      val Fbz = "RMB"

      val Fsssje = getFsssje(FBS: String, Fje: String, Fjsf: String, Fyhs: String, Fzgf: String, Fghf: String, Fqtf: String, Fqsghf: String) //TODO
      var Fzqbz = "HG"
      var Fywbz = ""

      FBS match {
        case "S" => Fywbz = "MRHG_GY"
        case "B" => Fywbz = "MCHG_GY"
      }

      SHjsmxHzjkqs(
        FSETID: String,
        Fdate: String,
        FinDate: String,
        Fzqdm: String,
        Fszsh: String,
        Fjyxwh: String,
        FBS: String,
        Fje: String,
        Fyj: String,
        Fjsf: String,
        Fyhs: String,
        Fzgf: String,
        Fghf: String,
        Fgzlx: String,
        Fhggain: String,
        Ffxj: String,
        Fsssje: String,
        Fzqbz: String,
        Fywbz: String,
        Fqsbz: String,
        Fqtf: String,
        Zqdm: String,
        Fjyfs: String,
        Fsh: String,
        Fzzr: String,
        Fchk: String,
        Fzlh: String,
        Ftzbz: String,
        Fqsghf: String,
        Fgddm: String,
        Fjybz: String,
        Isrtgs: String,
        Fpartid: String,
        Fhtxh: String,
        Fcshtxh: String,
        Frzlv: String,
        Fcsghqx: String,
        Fsjly: String,
        Fbz: String
      )
    })
    save2Mysql(GDSYGYHGTempRDD,sparkSession,"CDR_wmz")
  }


  /**
    * 回购-固定收益平台隔夜回购到期计算规则
    *
    * @param sparkSession
    * @param GDSYGYHGDQCalculateRDD
    * @param CSGDZHBroadCast
    * @param LSETLISTBroadCast
    */
  def GDSYGYHGDQCalculate(sparkSession: SparkSession, GDSYGYHGDQCalculateRDD: RDD[Row], CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]]): Unit = {
    val GDSYGYHGDQTempRDD =  GDSYGYHGDQCalculateRDD.map(row => {
      val Zqzh: String = row.getAs[String]("ZQZH")
      //获取资产代码
      val FSETID: String = getFSETID(Zqzh, CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]])
      val Fdate: String = getYWDate
      val FinDate: String = getYWDate

      var Fzqdm = row.getAs[String]("ZQDM1")
      val Fszsh: String = "G"
      val Fjyxwh: String = row.getAs[String]("XWH1")
      val FBS: String = row.getAs[String]("MMBZ")
      val Fje = row.getAs[String]("QSJE")
      val Fsl = BigDecimal(row.getAs[String]("SL")).abs.toString()
      val Fyj = "0"
      val Fjsf = getAbs(row, "JSF")
      val Fyhs = getAbs(row, "YHS")
      val Fzgf = getAbs(row, "ZHF")
      val Fghf = getAbs(row, "GHF")
      val Fgzlx = "0"
      val Fhggain = "0"
      val Ffxj = "0"

      val Fqsbz = "N"
      val Fqtf = "0"
      val Zqdm = row.getAs[String]("ZQDM1")
      val Fjyfs = "PT"
      val Fsh = "1"
      val Fzzr = " "
      val Fchk = " "
      val Fzlh = "0"
      val Ftzbz = " "
      val Fqsghf = "0"
      val Fgddm = Zqzh
      val Fjybz = " "
      val Isrtgs = "1"
      val Fpartid = " "
      val Fhtxh = " "
      val Fcshtxh = " "
      val Frzlv = "0"
      val Fcsghqx = "0"
      val Fsjly = " "
      val Fbz = "RMB"

      val Fsssje = getFsssje(FBS: String, Fje: String, Fjsf: String, Fyhs: String, Fzgf: String, Fghf: String, Fqtf: String, Fqsghf: String) //TODO
      var Fzqbz = "HG"
      var Fywbz = ""

      FBS match {
        case "S" => Fywbz = "MRHGDQ_GY"
        case "B" => Fywbz = "MCHGDQ_GY"
      }

      SHjsmxHzjkqs(
        FSETID: String,
        Fdate: String,
        FinDate: String,
        Fzqdm: String,
        Fszsh: String,
        Fjyxwh: String,
        FBS: String,
        Fje: String,
        Fyj: String,
        Fjsf: String,
        Fyhs: String,
        Fzgf: String,
        Fghf: String,
        Fgzlx: String,
        Fhggain: String,
        Ffxj: String,
        Fsssje: String,
        Fzqbz: String,
        Fywbz: String,
        Fqsbz: String,
        Fqtf: String,
        Zqdm: String,
        Fjyfs: String,
        Fsh: String,
        Fzzr: String,
        Fchk: String,
        Fzlh: String,
        Ftzbz: String,
        Fqsghf: String,
        Fgddm: String,
        Fjybz: String,
        Isrtgs: String,
        Fpartid: String,
        Fhtxh: String,
        Fcshtxh: String,
        Frzlv: String,
        Fcsghqx: String,
        Fsjly: String,
        Fbz: String
      )
    })
    save2Mysql(GDSYGYHGDQTempRDD,sparkSession,"CDR_wmz")
  }

  /**
    * 回购-买入卖出回购计算规则
    *
    * @param sparkSession
    * @param MRMCHGCalculateRDD
    * @param CSGDZHBroadCast
    * @param LSETLISTBroadCast
    * @param CSSYSYJLVBroadCast
    * @param LVARLISTBroadCast
    * @param CSSYSXWFYBroadCast
    * @param LSETCSSYSJJBroadCast
    * @param CSJYLVBroadCast
    */
  def MRMCHGCalculate(sparkSession: SparkSession, MRMCHGCalculateRDD: RDD[Row], CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]], CSSYSYJLVBroadCast: Broadcast[Array[String]], LVARLISTBroadCast: Broadcast[Array[String]], CSSYSXWFYBroadCast: Broadcast[Array[String]], LSETCSSYSJJBroadCast: Broadcast[Array[String]], CSJYLVBroadCast: Broadcast[Array[String]]): Unit = {
    var HGDay = ""
    var iFts = ""
    val MRMCHGTempRDD = MRMCHGCalculateRDD.map(row => {
      val Zqzh: String = row.getAs[String]("ZQZH")
      //获取资产代码
      val FSETID: String = getFSETID(Zqzh, CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]])
      val Fdate: String = getYWDate
      val FinDate: String = getYWDate

      var Fzqdm = row.getAs[String]("ZQDM1")
      val Fszsh: String = "H"
      val Fjyxwh: String = row.getAs[String]("XWH1")
      val FBS: String = row.getAs[String]("MMBZ")
      val Fje = row.getAs[String]("QSJE")
      val Fsl = BigDecimal(row.getAs[String]("SL")).abs.toString()

      var Fjsf = getAbs(row, "JSF")
      val Fyhs = getAbs(row, "YHS")
      val Fzgf = getAbs(row, "ZHF")
      val Fghf = getAbs(row, "GHF")
      val Fgzlx = "0"

      val Ffxj = "0"

      val Fqsbz = "N"
      val Fqtf = "0"
      val Zqdm = row.getAs[String]("ZQDM1")
      val Fjyfs = "PT"
      val Fsh = "1"
      val Fzzr = " "
      val Fchk = " "
      val Fzlh = "0"
      val Ftzbz = " "
      val Fqsghf = "0"
      val Fgddm = Zqzh
      val Fjybz = " "
      val Isrtgs = "1"
      val Fpartid = " "
      val Fhtxh = " "
      val Fcshtxh = " "
      val Frzlv = "0"
      val Fcsghqx = "0"
      val Fsjly = " "
      val Fbz = "RMB"


      /**
        * 回购佣金计算规则
        */

      var FLV = BigDecimal(0)
      var FLvmin = BigDecimal(0)
      var Flvzk = BigDecimal(0)

      val YjArr1 = CSSYSYJLVBroadCast.value.filter(lines => {
        lines(3).equals("HG" + Zqdm)
      })
      val YjArr2 = CSSYSYJLVBroadCast.value.filter(lines => {
        lines(3).equals("HG") && lines(8).equals(Fgddm)
      })
      val YjArr3 = CSSYSYJLVBroadCast.value.filter(lines => {
        lines(3).equals("HG") && lines(8).equals(Fjyxwh)
      })
      if (YjArr1 != null) {
        FLV = BigDecimal(YjArr1(5))
        FLvmin = BigDecimal(YjArr1(6))
        Flvzk = BigDecimal(YjArr1(12))
      } else if (YjArr1 == null && YjArr2 != null) {
        FLV = BigDecimal(YjArr1(5))
        FLvmin = BigDecimal(YjArr1(6))
        Flvzk = BigDecimal(YjArr1(12))
      } else if (YjArr1 == null && YjArr2 == null && YjArr3 != null) {
        FLV = BigDecimal(YjArr1(5))
        FLvmin = BigDecimal(YjArr1(6))
        Flvzk = BigDecimal(YjArr1(12))
      }

      val bJtlxValue: Array[String] = getLVARLISTValue(FSETID, "佣金包含经手费，证管费", LVARLISTBroadCast)
      var bJtlx = false
      if ("1".equals(bJtlxValue(1))) {
        bJtlx = true
      }

      //回购经手费
      val HGJSF = CSSYSXWFYBroadCast.value.filter(lines => {
        (lines(0).equals(Fgddm) || lines(0).equals(Fjyxwh)) &&
          lines(2).equals("HG") &&
          lines(3).equals("JSF")
      })(4)
      //回购征管费
      val HGZGF = CSSYSXWFYBroadCast.value.filter(lines => {
        (lines(0).equals(Fgddm) || lines(0).equals(Fjyxwh)) &&
          lines(2).equals("HG") &&
          lines(3).equals("ZGF")
      })(4)
      //回购过户费
      val HGGHF = CSSYSXWFYBroadCast.value.filter(lines => {
        (lines(0).equals(Fgddm) || lines(0).equals(Fjyxwh)) &&
          lines(2).equals("HG") &&
          lines(3).equals("GHF")
      })(4)
      //回购手续费
      val HGSXF = CSSYSXWFYBroadCast.value.filter(lines => {
        (lines(0).equals(Fgddm) || lines(0).equals(Fjyxwh)) &&
          lines(2).equals("HG") &&
          lines(3).equals("SXF")
      })(4)


      val YjTemp = (BigDecimal(Fje).abs * FLV).setScale(2, RoundingMode.HALF_UP)
      var FyjTemp1 = BigDecimal(0)
      var FyjTemp2 = BigDecimal(0)
      var FyjTemp3 = BigDecimal(0)


      if (!(bJtlx && HGJSF != 2)) {
        if (HGJSF == 0) {
          if (HGZGF == 0) {
            if (HGGHF == 0) {
              FyjTemp1 = BigDecimal(Fjsf) + BigDecimal(Fzgf) + BigDecimal(Fghf)
            } else {
              FyjTemp1 = BigDecimal(Fjsf) + BigDecimal(Fzgf) + 0
            }
          } else {
            if (HGGHF == 0) {
              FyjTemp1 = BigDecimal(Fjsf) + 0 + BigDecimal(Fghf)
            } else {
              FyjTemp1 = BigDecimal(Fjsf) + 0 + 0
            }
          }
        } else {
          if (HGZGF == 0) {
            if (HGGHF == 0) {
              FyjTemp1 = 0 + BigDecimal(Fzgf) + BigDecimal(Fghf)
            } else {
              FyjTemp1 = 0 + BigDecimal(Fzgf) + 0
            }
          } else {
            if (HGGHF == 0) {
              FyjTemp1 = 0 + 0 + BigDecimal(Fghf)
            } else {
              FyjTemp1 = 0 + 0 + 0
            }
          }
        }
      }

      FyjTemp1 = (YjTemp - FyjTemp1).setScale(2, RoundingMode.HALF_UP)

      if (Flvzk != 1) {
        FyjTemp1 = ((FyjTemp1 - BigDecimal(Ffxj)) * Flvzk).setScale(2, RoundingMode.HALF_UP)
      }


      val BYjZdzIsZeroValue: Array[String] = getLVARLISTValue(FSETID + Fjyxwh, "最低值为零", LVARLISTBroadCast)
      var BYjZdzIsZero = false
      if ("1".equals(BYjZdzIsZeroValue(1))) {
        BYjZdzIsZero = true
      }

      val BYjZdzValue: Array[String] = getLVARLISTValue(FSETID + Fjyxwh, "佣金少于低值按最低值", LVARLISTBroadCast)
      var BYjZdz = false
      if ("1".equals(BYjZdzValue(1))) {
        BYjZdz = true
      }


      if (BYjZdzIsZero && FyjTemp1 < 0) {
        FyjTemp1 = 0
      }

      if (BYjZdz && FyjTemp1 < FLvmin) {
        FyjTemp1 = FLvmin
      }

      //获取套账号
      val FSETCODE = CSGDZHBroadCast.value.filter(lines => {
        Zqzh.equals(lines(0))
      })(5)

      val LSETCSSYSJJArr = LSETCSSYSJJBroadCast.value.filter(lines => {
        lines(0).equals(FSETCODE)
      })

      val bHgYjValue: Array[String] = getLVARLISTValue(FSETID, "交易所回购计算佣金", LVARLISTBroadCast)
      var bHgYj = false
      if ("1".equals(bHgYjValue(1))) {
        bHgYj = true
      }

      if ("1 2 6".contains(LSETCSSYSJJArr(1)) && "0".equals(LSETCSSYSJJArr(3))) {
        if (!bHgYj) {
          Fjsf = FyjTemp1.toString()
          FyjTemp1 = 0
        }
      } else {
        if (bHgYj) {
          FyjTemp1 = 0
        } else {
          FyjTemp1 = BigDecimal(Fjsf)
        }
      }

      val Fyj = FyjTemp1.toString()

      /** 计算结束 **/


      //回购收益计算规则

      val fhgjewsValue: Array[String] = getLVARLISTValue(FSETID, "上海回购价格位数", LVARLISTBroadCast)
      val fhgjews = fhgjewsValue(1).toInt

      var Fhggain = ""

      val YWDate = DateUtils.formattedDate2Long(getYWDate, DateUtils.YYYYMMDD)
       HGDay = CSJYLVBroadCast.value.filter(lines => {
        lines(0).equals("HG" + Zqdm) && lines(2).equals("SXF") && lines(1).equals("H")
      })(6)

       iFts = subDay(getYWDate, getNextWorkDay(sparkSession, addDay(getYWDate, HGDay)))

      val JG2 = BigDecimal(row.getAs[String]("JG2"))
      if (Zqdm.startsWith("204")) {
        iFts = subDay(getNextWorkDay(sparkSession, addDay(getYWDate, iFts)), getNextWorkDay(sparkSession, getYWDate))
        Fhggain = ((JG2.*(BigDecimal(iFts))./(36500).setScale(fhgjews, RoundingMode.HALF_UP)).*(BigDecimal(Fje)).setScale(2, RoundingMode.HALF_UP)).toString()
      } else {
        Fhggain = ((JG2.*(BigDecimal(iFts))./(36500).setScale(fhgjews, RoundingMode.HALF_UP)).*(BigDecimal(Fje)).setScale(2, RoundingMode.HALF_UP)).toString()
      }

      /** 计算完成 **/


      val Fsssje = getFsssje(FBS: String, Fje: String, Fjsf: String, Fyhs: String, Fzgf: String, Fghf: String, Fqtf: String, Fqsghf: String) //TODO
      var Fzqbz = "HG"
      var Fywbz = ""

      FBS match {
        case "S" => Fywbz = "MRHGDQ_GY"
        case "B" => Fywbz = "MCHGDQ_GY"
      }

      SHjsmxHzjkqs(
        FSETID: String,
        Fdate: String,
        FinDate: String,
        Fzqdm: String,
        Fszsh: String,
        Fjyxwh: String,
        FBS: String,
        Fje: String,
        Fyj: String,
        Fjsf: String,
        Fyhs: String,
        Fzgf: String,
        Fghf: String,
        Fgzlx: String,
        Fhggain: String,
        Ffxj: String,
        Fsssje: String,
        Fzqbz: String,
        Fywbz: String,
        Fqsbz: String,
        Fqtf: String,
        Zqdm: String,
        Fjyfs: String,
        Fsh: String,
        Fzzr: String,
        Fchk: String,
        Fzlh: String,
        Ftzbz: String,
        Fqsghf: String,
        Fgddm: String,
        Fjybz: String,
        Isrtgs: String,
        Fpartid: String,
        Fhtxh: String,
        Fcshtxh: String,
        Frzlv: String,
        Fcsghqx: String,
        Fsjly: String,
        Fbz: String
      )
    })

    save2Mysql(MRMCHGTempRDD,sparkSession,"CDR_wmz")
    HGDQCalculate(sparkSession: SparkSession, CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]], HGDay: String, MRMCHGCalculateRDD: RDD[Row], iFts: String)
  }

  /**
    * 随着生成首期数据后自动生成到期数据，到期数据ETL及计算规则
    *
    * @param sparkSession
    * @param CSGDZHBroadCast
    * @param LSETLISTBroadCast
    * @param HGDay
    * @param MRMCHGCalculateRDD
    * @param iFts
    */
  def HGDQCalculate(sparkSession: SparkSession, CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]], HGDay: String, MRMCHGCalculateRDD: RDD[Row], iFts: String): Unit = {
    val HzjkqsDF: DataFrame = readMysql(sparkSession, "HZJKQS")
    val HzjkqsRDD: RDD[Row] = HzjkqsDF.rdd.filter(row => {
      getYWDate.equals(row.getAs[String]("FINDATE")) &&
        "HG".equals(row.getAs[String]("FZQBZ")) &&
        "MRHG,MCHG,MRHG_QY,MCHG_QY,MRBJHG,MCBJHG".contains(row.getAs[String]("FYWLB"))
    })

    val HGDQRDD = HzjkqsRDD.map(row => {
      //获取资产代码
      val FSETID: String = row.getAs[String]("FSETID")
      val Fdate: String = getNextWorkDay(sparkSession, addDay(getYWDate, HGDay))
      val FinDate: String = getYWDate

      var Fzqdm: String = row.getAs[String]("FZQDM")
      val Fszsh: String = row.getAs[String]("FSZSH")
      val Fjyxwh: String = row.getAs[String]("FJYXWH")
      var FBS: String = ""
      if ("B".equals(row.getAs[String]("FBS"))) {
        FBS = "S"
      } else {
        FBS = "B"
      }

      val Fje = row.getAs[String]("FJE")
      val Fsl = row.getAs[String]("FSL")
      val Fyj = "0"
      val Fjsf = "0"
      val Fyhs = "0"
      val Fzgf = "0"
      val Fghf = "0"
      val Fgzlx = "0"
      val Fhggain = row.getAs[String]("FHAAGIN")
      val Ffxj = "0"

      val Fqsbz = row.getAs[String]("FQSBZ")
      val Fqtf = "0"
      val Zqdm = row.getAs[String]("ZQDM")
      val Fjyfs = "PT"
      val Fsh = "1"
      val Fzzr = " "
      val Fchk = " "
      val Fzlh = "0"
      val Ftzbz = " "
      val Fqsghf = "0"
      val Fgddm = row.getAs[String]("FGDDM")
      val Fjybz = " "
      val Isrtgs = "1"
      val Fpartid = " "
      val Fhtxh = " "
      val Fcshtxh = " "
      val Frzlv = "0"
      val Fcsghqx = "0"
      val Fsjly = " "
      val Fbz = "RMB"

      val Fsssje = Fje + Fhggain
      var Fzqbz = row.getAs[String]("FZQBZ")
      var Fywbz = row.getAs[String]("RYWBZ") + "DQ"


      SHjsmxHzjkqs(
        FSETID: String,
        Fdate: String,
        FinDate: String,
        Fzqdm: String,
        Fszsh: String,
        Fjyxwh: String,
        FBS: String,
        Fje: String,
        Fyj: String,
        Fjsf: String,
        Fyhs: String,
        Fzgf: String,
        Fghf: String,
        Fgzlx: String,
        Fhggain: String,
        Ffxj: String,
        Fsssje: String,
        Fzqbz: String,
        Fywbz: String,
        Fqsbz: String,
        Fqtf: String,
        Zqdm: String,
        Fjyfs: String,
        Fsh: String,
        Fzzr: String,
        Fchk: String,
        Fzlh: String,
        Ftzbz: String,
        Fqsghf: String,
        Fgddm: String,
        Fjybz: String,
        Isrtgs: String,
        Fpartid: String,
        Fhtxh: String,
        Fcshtxh: String,
        Frzlv: String,
        Fcsghqx: String,
        Fsjly: String,
        Fbz: String
      )
    })

    save2Mysql(HGDQRDD,sparkSession,"CDR_wmz")
  }

  /**
    * 生成回购收益数据ETL及计算规则
    *
    * @param HzjkqsRDD
    * @param MRMCHGCalculateRDD
    * @param iFts
    */
  def HGSYCalculate(sparkSession: SparkSession, HzjkqsRDD: RDD[Row], MRMCHGCalculateRDD: RDD[Row], iFts: String): Unit = {
    var FSETID = ""
    var Fzqdm = ""
    var Fszsh = ""
    var Fjyxwh = ""
    var FBS = ""
    var Fje = ""
    var Fsl = ""
    var Fyj = ""
    var Fjsf = ""
    var Fyhs = ""
    var Fzgf = ""
    var Fghf = ""
    var Fgzlx = ""
    var Fhggain = ""
    var Ffxj = ""
    var Fsssje = ""
    var Fywbz = ""
    var Fqsbz = ""
    var Fqtf = ""
    var Zqdm = ""
    var Frzlv = ""
    var Fgddm = ""


    val Fcsghqx = iFts
    val Fzqbz = "HGSYXX"
    val Fdate = getYWDate
    val FinDate = getYWDate
    val Fjyfs = "XX"
    val Fsh = "1"
    val Fzzr = " "
    val Fchk = " "
    val Fzlh = "0"
    val Ftzbz = " "
    val Fqsghf = "0"
    val Fjybz = " "
    val Isrtgs = "1"
    val Fpartid = " "
    val Fhtxh = " "
    val Fcshtxh = " "
    val Fsjly = "jsmx"
    val Fbz = "RMB"

    val HzjkqsTempRDD: RDD[SHjsmxHzjkqs] = HzjkqsRDD.map(row => {
      FSETID = row.getAs[String]("FSETID")
      Fzqdm = row.getAs[String]("FZQDM")
      Fszsh = row.getAs[String]("FSZSH")
      Fjyxwh = row.getAs[String]("FJYXWH")
      Fje = row.getAs[String]("FJE")
      Fsl = row.getAs[String]("FSL")
      Fhggain = row.getAs[String]("FHGGAIN")
      Fywbz = row.getAs[String]("FYWBZ")
      Fqsbz = row.getAs[String]("FQSBZ")
      Zqdm = row.getAs[String]("ZQDM")

      SHjsmxHzjkqs(
        FSETID: String,
        Fdate: String,
        FinDate: String,
        Fzqdm: String,
        Fszsh: String,
        Fjyxwh: String,
        FBS: String,
        Fje: String,
        Fyj: String,
        Fjsf: String,
        Fyhs: String,
        Fzgf: String,
        Fghf: String,
        Fgzlx: String,
        Fhggain: String,
        Ffxj: String,
        Fsssje: String,
        Fzqbz: String,
        Fywbz: String,
        Fqsbz: String,
        Fqtf: String,
        Zqdm: String,
        Fjyfs: String,
        Fsh: String,
        Fzzr: String,
        Fchk: String,
        Fzlh: String,
        Ftzbz: String,
        Fqsghf: String,
        Fgddm: String,
        Fjybz: String,
        Isrtgs: String,
        Fpartid: String,
        Fhtxh: String,
        Fcshtxh: String,
        Frzlv: String,
        Fcsghqx: String,
        Fsjly: String,
        Fbz: String
      )
    })

    val MRMCHTempRDD: RDD[SHjsmxHzjkqs] = MRMCHGCalculateRDD.map(row => {
      FBS = row.getAs[String]("MMBZ")
      Fsssje = row.getAs[String]("JG2")
      Fgddm = row.getAs[String]("ZQZH")
      Frzlv = (BigDecimal(row.getAs[String]("JG2")) * BigDecimal(iFts) / (100 * 365)).setScale(5, RoundingMode.HALF_UP).toString()

      SHjsmxHzjkqs(
        FSETID: String,
        Fdate: String,
        FinDate: String,
        Fzqdm: String,
        Fszsh: String,
        Fjyxwh: String,
        FBS: String,
        Fje: String,
        Fyj: String,
        Fjsf: String,
        Fyhs: String,
        Fzgf: String,
        Fghf: String,
        Fgzlx: String,
        Fhggain: String,
        Ffxj: String,
        Fsssje: String,
        Fzqbz: String,
        Fywbz: String,
        Fqsbz: String,
        Fqtf: String,
        Zqdm: String,
        Fjyfs: String,
        Fsh: String,
        Fzzr: String,
        Fchk: String,
        Fzlh: String,
        Ftzbz: String,
        Fqsghf: String,
        Fgddm: String,
        Fjybz: String,
        Isrtgs: String,
        Fpartid: String,
        Fhtxh: String,
        Fcshtxh: String,
        Frzlv: String,
        Fcsghqx: String,
        Fsjly: String,
        Fbz: String
      )
    })

    val HGSYTempRDD: RDD[SHjsmxHzjkqs] = HzjkqsTempRDD.union(MRMCHTempRDD)

    save2Mysql(HGSYTempRDD,sparkSession,"CDR_wmz")

  }










  /**
    * 从mysql读取表数据
    *
    * @param sparkSession
    * @return
    */
  def readMysql(sparkSession: SparkSession, tableName: String): DataFrame = {
    val properties = new Properties()
    properties.put("driver", "com.mysql.jdbc.Driver")
    properties.setProperty("user", "root")
    properties.setProperty("password", "root1234")
    val tableDF: DataFrame = sparkSession.read.jdbc("jdbc:mysql://192.168.102.120:3306/JJCWGZ", tableName, properties)
    tableDF
  }


  /**
    * 时间戳转Date
    *
    * @param timeStamp
    * @return
    */
  def timeStamp2Date(timeStamp: Long): String = {
    val simpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val Date = simpleDateFormat.format(new Date(timeStamp))
    Date
  }

  /**
    * 日期相加
    *
    * @param startDate
    * @param Day
    * @return
    */
  def addDay(startDate: String, Day: String): String = {
    val startDateStamp = DateUtils.formattedDate2Long(startDate, DateUtils.YYYYMMDD)
    val DayStamp = (Day.toLong) / 24 / 60 / 60 / 1000
    val sumDayStamp = startDateStamp + DayStamp

    timeStamp2Date(sumDayStamp)
  }

  /**
    * 日期相减
    *
    * @param startDate
    * @param endDate
    * @return
    */
  def subDay(startDate: String, endDate: String): String = {
    val startDateStamp = DateUtils.formattedDate2Long(startDate, DateUtils.YYYYMMDD)
    val endDateStamp = DateUtils.formattedDate2Long(endDate, DateUtils.YYYYMMDD)
    val subDayStamp = startDateStamp - endDateStamp

    (subDayStamp / 1000 / 60 / 60 / 24).toString
  }

  /**
    * 根据套账号及参数名称获取默认值
    *
    * @param FSETCODE
    * @param Str
    * @param LVARLISTBroadCast
    * @return
    */
  def getLVARLISTValue(FSETCODE: String, Str: String, LVARLISTBroadCast: Broadcast[Array[String]]): Array[String] = {
    LVARLISTBroadCast.value.filter(lines => {
      (FSETCODE + Str).equals(lines(0))
    })
  }


  /**
    * 计算卖实收金额
    *
    * @param Fjsf
    * @param Fyhs
    * @param Fzgf
    * @param Fghf
    * @param Fqtf
    * @param Fqsghf
    * @return
    */
  def getFsssje(FBS: String, Fje: String, Fjsf: String, Fyhs: String, Fzgf: String, Fghf: String, Fqtf: String, Fqsghf: String): String = {
    var Fsssje = ""
    if ("B".equals(FBS)) {
      Fsssje = (BigDecimal(Fje) + BigDecimal(Fjsf) + BigDecimal(Fyhs) + BigDecimal(Fzgf) + BigDecimal(Fghf) + BigDecimal(Fqtf) + BigDecimal(Fqsghf)).toString()
    } else if ("S".equals(FBS)) {
      Fsssje = (BigDecimal(Fje) - BigDecimal(Fjsf) + BigDecimal(Fyhs) + BigDecimal(Fzgf) + BigDecimal(Fghf) + BigDecimal(Fqtf) + BigDecimal(Fqsghf)).toString()
    }
    Fsssje
  }


  /**
    * 获取标志
    *
    * @param num
    * @param zqdm
    * @param str1
    * @param str2
    * @return
    */
  def getBz(num: String, zqdm: String, str1: String, str2: String): String = {
    var Bz = ""
    if (zqdm.startsWith(num)) {
      Bz = str1
    } else {
      Bz = str2
    }
    Bz
  }

  /**
    * 乘法运算
    *
    * @param str1
    * @param str2
    * @return
    */
  def mul(str1: String, str2: String, num: Int): String = {
    (BigDecimal(str1) * BigDecimal(str2)).setScale(num, RoundingMode.HALF_UP).toString()
  }

  /**
    * 替换字符串
    *
    * @param str1
    * @param str2
    * @return
    */
  def replaceString(str1: String, str2: String): String = {
    val strTemp = str1.substring(3, str1.length)
    val str = str2 + strTemp
    str
  }

  /**
    * 获取下一个工作日
    *
    * @param sparkSession
    * @return
    */
  def getNextWorkDay(sparkSession: SparkSession, YWDate: String): String = {
    val sc = sparkSession.sqlContext
    val csholidayPath = Util.getDailyInputFilePath(TABLE_NAME_HOLIDAY)
    val csholidayList = sc.sparkContext.textFile(csholidayPath)
      .filter(str => {
        val fields = str.split(",")
        val fdate = fields(0)
        val fbz = fields(1)
        val fsh = fields(3)
        if (DEFAULT_VALUE_0.equals(fbz) && FSH.equals(fsh) && fdate.compareTo(YWDate) >= 0) true
        else false
      })
      .map(str => {
        str.split(SEPARATE2)(0)
      }).takeOrdered(1)
    if (csholidayList.length == 0) YWDate
    else csholidayList(0)
  }

  /**
    * 获取绝对值
    *
    * @param string
    * @return
    */
  def getAbs(row: Row, String: String): String = {
    BigDecimal(row.getAs[String]("String")).abs.toString()
  }

  /**
    * 获取业务日期 TODO
    *
    * @return
    */
  def getYWDate: String = {
    ""
  }


  /**
    * 根据股东代码（zqzh）去csgdzh表查套账号，根据套账号去lsetlist查资产代码
    *
    * @param Zqzh
    * @param CSGDZHBroadCast
    * @param LSETLISTBroadCast
    * @return
    */
  def getFSETID(Zqzh: String, CSGDZHBroadCast: Broadcast[Array[String]], LSETLISTBroadCast: Broadcast[Array[String]]): String = {
    //println(CSGDZHBroadCast.value.toBuffer)
    val CSGDZHFilterArr: Array[String] = CSGDZHBroadCast.value.filter(lines => {
      Zqzh.equals(lines(0))
    })
    //println(CSGDZHFilterArr.toBuffer)
    var FSETID = " "
    val LSETLISTFilterArr: Array[String] = LSETLISTBroadCast.value.filter(lines => {
      CSGDZHFilterArr(5).equals(lines(2))
    })
    FSETID = LSETLISTFilterArr(1)

    FSETID
}

  /**
    * RDD转换DF
    * @param targetRDD
    * @param sparkSession
    * @return
    */
  def RDD2DF(targetRDD: RDD[SHjsmxHzjkqs],sparkSession: SparkSession): DataFrame = {
    // 将费率表转换成DateFrame
    val schemaString: String = "FSETID,Fdate,FinDate,FZqdm,FSzsh,Fjyxwh,FBS,Fje,Fyj,Fjsf,Fyhs,Fzgf,Fghf,Fgzlx,Fhggain,Ffxj,Fsssje,Fzqbz,Fywbz,Fqsbz,Fqtf,Zqdm,Fjyfs,Fsh,Fzzr,Fchk,Fzlh,Ftzbz,Fqsghf,Fgddm,Fjybz,Isrtgs,Fpartid,Fhtxh,Fcshtxh,Frzlv,Fcsghqx,Fsjly,Fbz"
    val fields: Array[StructField] = schemaString.split(",").map(fieldname => StructField(fieldname, StringType, nullable = true))
    val schema = StructType(fields)
    val rowRDD= targetRDD.map(_.toString.split(",")).map(attributes => Row(attributes(0).trim, attributes(1).trim, attributes(2).trim, attributes(3).trim, attributes(4).trim, attributes(5).trim, attributes(6).trim, attributes(7).trim, attributes(8).trim, attributes(9).trim, attributes(10).trim, attributes(11).trim, attributes(12).trim, attributes(13).trim, attributes(14).trim, attributes(15).trim, attributes(16).trim, attributes(17).trim, attributes(18).trim, attributes(19).trim, attributes(20).trim, attributes(21).trim, attributes(22).trim, attributes(23).trim, attributes(24).trim, attributes(25).trim, attributes(26).trim, attributes(27).trim, attributes(28).trim, attributes(29).trim, attributes(30).trim, attributes(31).trim, attributes(32).trim, attributes(33).trim, attributes(34).trim, attributes(35).trim, attributes(36).trim, attributes(37).trim, attributes(38).trim, attributes(39).trim))
    val targetDF: DataFrame = sparkSession.createDataFrame(rowRDD, schema)
    targetDF
  }

  /**
    * 存储到mySql
    * @param targetRDD
    * @param sparkSession
    * @param tableName
    */
  def save2Mysql(targetRDD: RDD[SHjsmxHzjkqs],sparkSession: SparkSession,tableName: String) = {
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "root1234")

    RDD2DF(targetRDD,sparkSession).write.mode(SaveMode.Append).jdbc("jdbc:mysql://192.168.102.120:3306/JJCWGZ", "tableName", properties)
  }



  /**
    * 过滤原始数据
    * @param SHjsmxFileDF
    * @return
    */
  def FilterDF2RDD(SHjsmxFileDF: DataFrame): RDD[Row] = {
    val SHjsmxFileRDD: RDD[Row] = SHjsmxFileDF.rdd.filter(row => {
      "20121105".equals(row.getAs[String]("QSRQ")) ||
        ("20121106".equals(row.getAs[String]("JSRQ")) && "003".equals(row.getAs[String]("JLLX")) && "001".equals(row.getAs[String]("YWLX"))) ||
        ("20181106".equals(row.getAs[String]("QTRQ")) && "990".equals(row.getAs[String]("JLLX")) && "802".equals(row.getAs[String]("YWLX"))) ||
        (row.getAs[String]("FJSM").contains("恢复交收"))
    })
    SHjsmxFileRDD
  }





}

