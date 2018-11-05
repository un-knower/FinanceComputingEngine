package com.yss.scala.core

import java.io.File
import java.util.Properties

import com.yss.scala.dto.SHFICCTriPartyRepoDto
import com.yss.scala.util.{DateUtils, RowUtils, Util}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

/**
  * @auther: lijiayan
  * @date: 2018/9/17
  * @desc: 上海固定收益平台三方回购业务
  *        源文件:jsmx和wdq清洗后的数据
  *        目标表:
  */
object ShFICCTriPartyRepo {

  //存储结果数据的数据库链接
  private val MYSQL_JDBC_URL = "jdbc:mysql://192.168.21.110:3306/yss"
  //存储结果数据的表名
  private val MYSQL_RESULT_TABLE_NAME = "sfhg"
  //jdbc驱动累
  private val DRIVER_CLASS = "com.mysql.jdbc.Driver"

  private val MYSQL_USER = "root"
  private val MYSQL_PASSWD = "root"

  //etl后的数据路径
  val ETL_DATA_PATH = "hdfs://192.168.21.110:9000/guzhi/etl/sfgu/"

  //佣金利率表
  private val YJLL_TABLE = "A001CSYJLV"

  //参数列表表，比如某个参数是否选中 如 交易所回购计算佣金选项是否选中，选中为1，其他为0
  private val PARAMS_LIST_TABLE = "LVARLIST"

  //席位号表
  private val XHW_TABLE = "CSQSXW"

  //股东代码表
  private val GUDM_TABLE = "CSGDZH"

  //资产代码表
  private val ZC_TABLE = "LSETLIST"

  //要使用的表在hdfs中的路径
  private val TABLE_HDFS_PATH = "hdfs://192.168.102.120:8020/yss/guzhi/basic_list/"


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName(getClass.getSimpleName)
      //.master("local[*]")
      .getOrCreate()

    //readETLDataFromJDBC(spark)
    // 读取etl后的csv文件并转化成RDD
    val path = ETL_DATA_PATH + DateUtils.formatDate(System.currentTimeMillis())
    val dataDF: DataFrame = readETLDataFromJDBC(spark)
    exec(spark, dataDF)
    spark.stop()

  }


  def exec(spark: SparkSession, dataDF: DataFrame): Unit = {
    val sfhgDataRDD = /*Util.readCSV(path, spark)*/ dataDF.rdd.map(row => {
      //val xwh = RowUtils.getRowFieldAsString(row, "XWH1")
      val fgddm = RowUtils.getRowFieldAsString(row, "ZQZH")
      (fgddm, row)
    })

    // 读取csqsxw,并得到<席位号or股东代码,套账号>的RDD
    val xwhAndTzhRDD: RDD[(String, String)] = readCSGDZH(spark) //readCSQSXW(spark)

    //得到<席位号,<dataRow,Option(套账号)>>
    val xwh2DataAndTZH: RDD[(String, (Row, Option[String]))] = sfhgDataRDD.leftOuterJoin(xwhAndTzhRDD)

    //将xwh2DataAndTZH 转化成 (套账号+"交易所回购计算佣金",(dataRow,套账号))
    val tzh_selectItemName2DataRowAndTzh: RDD[(String, (Row, String))] = xwh2DataAndTZH.map(item => {
      val tzh = item._2._2.getOrElse("")
      (tzh + "交易所回购计算佣金", (item._2._1, tzh))
    })

    //读取参数表
    val tzh_selectItemName2SelectResult: RDD[(String, Boolean)] = readLVARLIST(spark)

    // <数据,套账号,选项结果>
    val rowDataAndTzhAndSelected: RDD[(Row, String, Boolean)] = tzh_selectItemName2DataRowAndTzh.leftOuterJoin(tzh_selectItemName2SelectResult).map(item => {
      (item._2._1._1, item._2._1._2, item._2._2.getOrElse(false))
    })

    //读取佣金表,得到<证券类别|席位号|市场号,佣金利率>
    val zqlbAndXwhAndSC2YjFV: RDD[(String, String)] = readA117CSYJLV(spark)

    //<证券类别|席位号|市场号,(数据,套账号,选项结果)>
    val zqlbAndXwhAndSC2RowDataAndTzhAndSelected: RDD[(String, (Row, String, Boolean))] = rowDataAndTzhAndSelected.map(item => {
      val xwh = RowUtils.getRowFieldAsString(item._1, "XWH1")
      ("ZQZYSFHG|" + xwh + "|G", item)
      /*("GP|" + "000001" + "|S", item)*/
    })

    //<套账号,数据,选项结果,佣金利率>
    val rowDataAndTzhAndSelectedAndYjFV: RDD[(String, (Row, Boolean, String))] = zqlbAndXwhAndSC2RowDataAndTzhAndSelected.leftOuterJoin(zqlbAndXwhAndSC2YjFV).map(item => {
      //(item._2._1._1, item._2._1._2, item._2._1._3, item._2._2.getOrElse("0"))
      (item._2._1._2,(item._2._1._1, item._2._1._3, item._2._2.getOrElse("0")))
    })

    //读取资产表 得到<套账号,资产id>
    val tzhAndZCID: RDD[(String, String)] = readLSETLIST(spark)

    val rowDataAndTzhAndSelectedAndYjFVAndZCId: RDD[(Row, String, Boolean, String, String)] = rowDataAndTzhAndSelectedAndYjFV.leftOuterJoin(tzhAndZCID).map(item => {
      (item._2._1._1, item._1, item._2._1._2, item._2._1._3, item._2._2.getOrElse(""))
    })


    //开始计算
    val resSFHGRDD: RDD[SHFICCTriPartyRepoDto] = calculate(rowDataAndTzhAndSelectedAndYjFVAndZCId)

    import spark.implicits._

    // val properties = new Properties()
    // properties.put("user", MYSQL_USER)
    // properties.put("password", MYSQL_PASSWD)
    // properties.setProperty("driver", DRIVER_CLASS)
    // resSFHGRDD.toDF().write.mode(SaveMode.Overwrite).jdbc(MYSQL_JDBC_URL, MYSQL_RESULT_TABLE_NAME, properties)
    //
    resSFHGRDD.toDF().show()

    saveToMySQL(spark, resSFHGRDD)
  }

  def saveToMySQL(spark: SparkSession, resSFHGRDD: RDD[SHFICCTriPartyRepoDto]): Unit = {
    import spark.implicits._
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "root1234")
    properties.put("driver", "com.mysql.jdbc.Driver")
    val url = "jdbc:mysql://192.168.102.120:3306/JJCWGZ"
    resSFHGRDD.toDF().write.mode(SaveMode.Overwrite).jdbc(url, "ShFICCTriPartyRepo", properties)
  }

  /**
    * 读取清洗后的数据
    */
  def readETLDataFromJDBC(spark: SparkSession) = {
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "root1234")
    properties.put("driver", DRIVER_CLASS)
    spark.sqlContext.read.jdbc("jdbc:mysql://192.168.102.120:3306/JJCWGZ", "JSMX03_WDQ_ETL", properties)
    /*.toDF().show()*/
  }

  /**
    * 开始计算
    *
    * @param rowDataAndTzhAndSelectedAndYjFV opp RDD
    * @return
    *
    */
  def calculate(rowDataAndTzhAndSelectedAndYjFV: RDD[(Row, String, Boolean, String,String)]): RDD[SHFICCTriPartyRepoDto] = {
    rowDataAndTzhAndSelectedAndYjFV.map(item => {
      val row = item._1
      val tzh = item._2
      val isSelected = item._3
      val fv = item._4
      //资产ID
      val zcID = item._5
      val FDate = RowUtils.getRowFieldAsString(row, "JYRQ")

      val FZqdm = RowUtils.getRowFieldAsString(row, "ZQDM1")

      val FSzsh = "G"

      val FJyxwh = RowUtils.getRowFieldAsString(row, "XWH1")

      val FZqbz = RowUtils.getRowFieldAsString(row, "FZQBZ")

      val Fjybz = RowUtils.getRowFieldAsString(row, "FJYBZ")

      val ZqDm = RowUtils.getRowFieldAsString(row, "ZQDM1")

      val MMBZ = RowUtils.getRowFieldAsString(row, "MMBZ")
      var FJyFs = " "
      if ("B".equals(MMBZ)) {
        FJyFs = "RZ"
      } else if ("S".equals(MMBZ)) {
        FJyFs = "CZ"
      }

      val Fsh = "1"

      val Fzzr = "admin"

      val Fchk = "admin"

      val FHTXH = RowUtils.getRowFieldAsString(row, "CJBH")

      val FSETID = zcID

      val FRZLV = BigDecimal(RowUtils.getRowFieldAsString(row, "JG1"))

      val FSJLY = "ZD"

      val FBS = MMBZ

      val FSL = BigDecimal(0.00)

      val Fyhs = BigDecimal(RowUtils.getRowFieldAsString(row, "YHS")).abs

      val Fzgf = BigDecimal(RowUtils.getRowFieldAsString(row, "ZGF")).abs

      val Fghf = BigDecimal(RowUtils.getRowFieldAsString(row, "GHF")).abs

      val FFxj = BigDecimal(0.00)

      val FQtf = BigDecimal(0.00)


      val Fgzlx = BigDecimal(0.00)

      val FQsbz = " "

      val ftzbz = " "

      val FQsghf = BigDecimal(0.00)

      val FGddm = RowUtils.getRowFieldAsString(row, "ZQZH")

      val fzlh = " "

      val ISRTGS = " "

      val FPARTID = " "

      val FYwbz = " "

      val Fbz = " "

      //qtrq-cjrq
      val QTRQCJRQ = RowUtils.getRowFieldAsString(row, "QTRQCJRQ")

      val YWLX = RowUtils.getRowFieldAsString(row, "YWLX")

      //=====================上面是公有变量,下面是根据业务类型变化的变量计算================================
      var FInDate = ""

      var Fje = BigDecimal(0.00)

      var Fyj = BigDecimal(0.00)

      var Fjsf = BigDecimal(RowUtils.getRowFieldAsString(row, "JSF")).abs

      //初始回购期限
      var FCSGHQX = BigDecimal(0.00)

      var FCSHTXH = ""

      var FSSSFJE = BigDecimal(0.00)

      if ("680".equals(YWLX)) {
        FInDate = RowUtils.getRowFieldAsString(row, "QTRQ")
        Fje = BigDecimal(RowUtils.getRowFieldAsString(row, "QSJE")).abs

        FCSGHQX = BigDecimal(DateUtils.absDays(FInDate, FDate))
        if (isSelected) {
          Fyj = Fje * BigDecimal(fv)
        }
        FSSSFJE = BigDecimal(RowUtils.getRowFieldAsString(row, "SJSF")).abs
        FCSHTXH = RowUtils.getRowFieldAsString(row, "CJBH")
      }
      else if ("681".equals(YWLX)) {
        FInDate = RowUtils.getRowFieldAsString(row, "JYRQ")
        FCSGHQX = BigDecimal(DateUtils.absDays(FInDate, RowUtils.getRowFieldAsString(row, "QTRQ")))
        Fje = BigDecimal(RowUtils.getRowFieldAsString(row, "QSJE")).abs / (1 + BigDecimal(RowUtils.getRowFieldAsString(row, "JG1")) / 100 * FCSGHQX / 365)
        FSSSFJE = BigDecimal(RowUtils.getRowFieldAsString(row, "SJSF")).abs
        FCSHTXH = RowUtils.getRowFieldAsString(row, "SQBH")
      }
      else if ("683".equals(YWLX)) {
        FInDate = RowUtils.getRowFieldAsString(row, "JYRQ")
        FCSGHQX = BigDecimal(DateUtils.absDays(FInDate, RowUtils.getRowFieldAsString(row, "QTRQ")))
        Fje = BigDecimal(RowUtils.getRowFieldAsString(row, "QSJE")).abs / (1 + FRZLV / 100 * FCSGHQX / 365)
        FSSSFJE = BigDecimal(RowUtils.getRowFieldAsString(row, "SJSF")).abs
        FCSHTXH = RowUtils.getRowFieldAsString(row, "SQBH")
      }
      //682
      else {
        FCSHTXH = RowUtils.getRowFieldAsString(row, "SQBH")
        //682续作合约新开数据取值规则
        if ("XZXK_SFHG".equals(Fjybz)) {
          FInDate = DateUtils.addDays(FDate, QTRQCJRQ.toInt)
          FCSGHQX = BigDecimal(QTRQCJRQ)
          Fje = BigDecimal(RowUtils.getRowFieldAsString(row, "QSJE")).abs

          if (isSelected) {
            Fyj = Fje * BigDecimal(fv)
          }
          FSSSFJE = BigDecimal(RowUtils.getRowFieldAsString(row, "QSJE")).abs

        }
        //682续作前期合约了结数据取值规则
        else {
          FInDate = FDate
          FCSGHQX = BigDecimal(QTRQCJRQ)
          Fje = BigDecimal(RowUtils.getRowFieldAsString(row, "QTJE1")).abs / (1 + FRZLV.setScale(4, BigDecimal.RoundingMode.HALF_UP) / 100 * FCSGHQX / 365)
          Fyj = BigDecimal(0.00).setScale(2, BigDecimal.RoundingMode.HALF_UP)
          Fjsf = BigDecimal(0.00).setScale(2, BigDecimal.RoundingMode.HALF_UP)
          FSSSFJE = BigDecimal(RowUtils.getRowFieldAsString(row, "QTJE1")).setScale(2, BigDecimal.RoundingMode.HALF_UP).abs
        }

      }

      val FHggain = Fje * FRZLV / 100 * FCSGHQX / 365

      SHFICCTriPartyRepoDto(
        FDate,
        FInDate,
        FZqdm,
        FSzsh,
        FJyxwh,
        Fje.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString(),
        Fyj.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString(),
        Fjsf.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString(),
        FHggain.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString(),
        FSSSFJE.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString(),
        FZqbz,
        Fjybz,
        ZqDm,
        FJyFs,
        Fsh,
        Fzzr,
        Fchk,
        FHTXH,
        FSETID,
        FCSGHQX.setScale(0, BigDecimal.RoundingMode.HALF_UP).toString(),
        FRZLV.setScale(4, BigDecimal.RoundingMode.HALF_UP).toString(),
        FSJLY,
        FCSHTXH,
        FBS,
        FSL.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString(),
        Fyhs.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString(),
        Fzgf.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString(),
        Fghf.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString(),
        FFxj.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString(),
        FQtf.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString(),
        Fgzlx.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString(),
        FQsbz,
        ftzbz,
        FQsghf.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString(),
        FGddm,
        fzlh,
        ISRTGS,
        FPARTID,
        FYwbz,
        Fbz
      )

    })
  }

  /**
    * 读取佣金利率表 A117CSYJLV
    *
    * @param spark SparkSession
    * @return <证券类别|席位号|市场号,佣金>
    */
  def readA117CSYJLV(spark: SparkSession): RDD[(String, String)] = {
    Util.readCSV(getTableDataPath(YJLL_TABLE), spark, header = false, ",").toDF(
      "FID",
      "FZQLB",
      "FSZSH",
      "FLV",
      "FLVMIN",
      "FTJ1",
      "FSTR1",
      "FTJ2",
      "FTJ2FROM",
      "FTJ2TO",
      "FLVZK",
      "FSH",
      "FZZR",
      "FCHK",
      "FSTARTDATE",
      "FJJDM",
      "FGDJE"
    ).rdd.map(row => {
      val FZQLB = RowUtils.getRowFieldAsString(row, "FZQLB")
      val FSZSH = RowUtils.getRowFieldAsString(row, "FSZSH")
      val FSTR1 = RowUtils.getRowFieldAsString(row, "FSTR1")
      val FLV = RowUtils.getRowFieldAsString(row, "FLV", "0")
      val FLVZK = RowUtils.getRowFieldAsString(row, fieldName = "FLVZK", defaultValue = "1")
      val resFV = (BigDecimal(FLV) * BigDecimal(FLVZK)).toString()
      (FZQLB + "|" + FSTR1 + "|" + FSZSH, resFV)
    })
  }


  /**
    * 读取参数表 lvarlist  参数表
    *
    * @param spark SparkSession
    * @return <选项名称,是否选中(true/false)>
    */
  def readLVARLIST(spark: SparkSession): RDD[(String, Boolean)] = {
    Util.readCSV(getTableDataPath(PARAMS_LIST_TABLE), spark, header = false, ",").toDF(
      "FVARNAME",
      "FVARVALUE",
      "FSH",
      "FZZR",
      "FCHK",
      "FSTARTDATE"
    ).rdd.map(row => {
      val FVARNAME = RowUtils.getRowFieldAsString(row, "FVARNAME")
      val FVARVALUE = RowUtils.getRowFieldAsString(row, "FVARVALUE")

      var checked = false
      if ("1".equals(FVARVALUE)) checked = true
      (FVARNAME, checked)
    })
  }


  /**
    * 读取 csqsxw    席位表
    *
    * @param spark SparkSession
    * @return 返回<席位号,套账号>
    */
  def readCSQSXW(spark: SparkSession): RDD[(String, String)] = {
    Util.readCSV(getTableDataPath(XHW_TABLE), spark, header = false, ",").toDF(
      "FQSDM",
      "FQSMC",
      "FSZSH",
      "FQSXW",
      "FXWLB",
      "FSETCODE",
      "FSH",
      "FZZR",
      "FCHK",
      "FSTARTDATE"
    ).rdd.map(row => {
      val xwh = RowUtils.getRowFieldAsString(row, "FQSXW")
      val tzh = RowUtils.getRowFieldAsString(row, "FSETCODE")
      val FSTARTDATE = RowUtils.getRowFieldAsString(row, "FSTARTDATE")
      (xwh, (tzh, DateUtils.formattedDate2Long(FSTARTDATE, DateUtils.YYYY_MM_DD)))
    }).groupByKey().map(item => {
      //由于席位号和套账号相同的情况会有多个,得根据日期来取最大值
      (item._1, item._2.toList.sortBy(tup => tup._2).reverse.head)
    }).map(item => {
      (item._1, item._2._1)
    })
  }


  /**
    * 读取 CSGDZH 股东代码表
    *
    * @param spark SparkSession
    * @return 放回<股东代码,套账号>
    */
  def readCSGDZH(spark: SparkSession): RDD[(String, String)] = {
    Util.readCSV(getTableDataPath(GUDM_TABLE), spark, header = false, ",").toDF(
      "FGDDM",
      "FGDXM",
      "FSZSH",
      "FSH",
      "FZZR",
      "FSETCODE",
      "FCHK",
      "FSTARTDATE",
      "FACCOUNTTYPT"
    ).rdd.map(row => {
      val gddm = RowUtils.getRowFieldAsString(row, "FGDDM")
      val tzh = RowUtils.getRowFieldAsString(row, "FSETCODE")
      val FSTARTDATE = RowUtils.getRowFieldAsString(row, "FSTARTDATE")
      (gddm, (tzh, DateUtils.formattedDate2Long(FSTARTDATE, DateUtils.YYYY_MM_DD)))
    }).groupByKey().map(item => {
      //由于席股东代码和套账号相同的情况会有多个,得根据日期来取最大值
      (item._1, item._2.toList.sortBy(tup => tup._2).reverse.head)
    }).map(item => {
      (item._1, item._2._1)
    })
  }


  /**
    * 读取资产代码表
    *
    * @param spark SparkSession
    * @return
    */
  def readLSETLIST(spark: SparkSession): RDD[(String, String)] = {
    Util.readCSV(getTableDataPath(ZC_TABLE), spark, header = false, ",").toDF(
      "FYEAR",
      "FSETID",
      "FSETCODE",
      "FSETNAME",
      "FMANAGER",
      "FSTARTYEAR",
      "FSTARTMONTH",
      "FMONTH",
      "FACCLEN",
      "FSTARTED",
      "FDJJBZ",
      "FPSETCODE",
      "FSETLEVEL",
      "FTSETCODE",
      "FSH",
      "FZZR",
      "FCHK",
      "FTZJC",
      "FZYDM",
      "FTZZHDM"
    ).rdd.map(row => {
      val zhid = RowUtils.getRowFieldAsString(row, "FSETID")
      val tzh = RowUtils.getRowFieldAsString(row, "FSETCODE")
      (tzh, zhid)
    })
  }


  /**
    * 根据表名获取表在hdfs上对应的路径
    *
    * @param tName :表面
    * @return
    */
  def getTableDataPath(tName: String): String = {
    val date = DateUtils.formatDate(System.currentTimeMillis())
    TABLE_HDFS_PATH + date + File.separator + tName
  }
  
}
