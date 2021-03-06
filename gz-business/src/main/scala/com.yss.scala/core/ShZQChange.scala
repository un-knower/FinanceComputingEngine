package com.yss.scala.core

import java.io.File
import java.util.Properties

import com.yss.scala.dto.{Hzjkqs, ShZQBD}
import com.yss.scala.util.{DateUtils, RowUtils, FceUtils, BasicUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks

/**
  * @auther: lijiayan
  * @date: 2018/11/5
  * @desc: 上海证券变动
  */
//noinspection ScalaDocParserErrorInspection
object ShZQChange {

  //源数据
  val ZQBD_basePath = "hdfs://192.168.102.120:8020/yss/guzhi/interface/"

  //要使用的表在hdfs中的路径
  private val TABLE_HDFS_PATH = "hdfs://192.168.102.120:8020/yss/guzhi/basic_list/"

  //结果存到hdfs的路径
  private val RES_HDFS_PATH = "hdfs://192.168.102.120:8020//yss/guzhi/hzjkqs/"


  //用于存储股东代码表中每一个股东代码对应的套账号
  var fgddm2Fsetcode: Map[String, String] = _

  //用于存储资产表中每一个套账号对应的资产id
  var fsetcode2Fsetid: Map[String, String] = _

  var CSKZZHS_data: Array[(String, String, String, String, String, String)] = _

  var zqdm2Fscdm: Map[String, String] = _

  var selectKey2Value: Map[String, String] = _

  var CSQYXX_data: Array[(String, String, String, String, String, String, String)] = _

  var JJGZLX_data: Array[(String, String, String)] = _

  var CSJYLV_data: Array[(String, String, String, String, String)] = _

  var CSSYSTSKM_data: Array[(String, String, String, String)] = _

  var NEXT_WORK_DAY: String = _


  def main(args: Array[String]): Unit = {

    var ywrq = DateUtils.formatDate(System.currentTimeMillis())

    if (args == null || args.length != 1) {
      throw new IllegalArgumentException("请传入业务日期")
    }
    ywrq = args(0)

    val spark = SparkSession.builder()
      .appName(getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    var dataPath = ZQBD_basePath + ywrq + "/zqbd/zqbd*.tsv"

    val zqdbOrgData: RDD[Row] = readZQBDData(spark, dataPath)

    //将数据按照条件进行过滤
    val zqbdData: RDD[Row] = filterData(zqdbOrgData, ywrq)

    zqbdData.persist()
    //读取股东账号表
    readCSGDZH(spark)

    //读取基金资产信息表
    readLSETLIST(spark)

    //读取回售信息新表
    readCSKZZHS(spark)

    //读取债券信息表
    readCSZQXX(spark)

    //读取参数信息表
    readLVARLIST(spark)

    readCSQYXX(spark)

    //读取国债利息表
    readJJGZLX(spark)

    //读取交易费率表
    readCSJYLV(spark)

    //读取下一个工作日
    NEXT_WORK_DAY = FceUtils.getCsholiday(spark.sparkContext, DateUtils.changeDateForm(ywrq, DateUtils.YYYYMMDD, DateUtils.YYYY_MM_DD))

    NEXT_WORK_DAY = DateUtils.changeDateForm(NEXT_WORK_DAY, DateUtils.YYYY_MM_DD, DateUtils.YYYYMMDD)
    //读取特殊科目表
    readCSSYSTSKM(spark)
    //进行计算
    caculate(spark, zqbdData, ywrq)

    spark.stop()

  }


  private def readCSSYSTSKM(spark: SparkSession): Unit = {
    CSSYSTSKM_data = BasicUtils.readCSV(getTableDataPath("CSSYSTSKM"), spark, header = false, sep = ",").toDF(
      "FSETCODE",
      "FSETID",
      "FZQDM",
      "FBZ",
      "FSH",
      "FZZR",
      "FCHK",
      "FSTARTDATE",
      "IP/DB"
    ).rdd.map(row => {
      val FZQDM = RowUtils.getRowFieldAsString(row, "FZQDM")
      val FBZ = RowUtils.getRowFieldAsString(row, "FBZ")
      val FSH = RowUtils.getRowFieldAsString(row, "FSH")
      val FSTARTDATE = RowUtils.getRowFieldAsString(row, "FSTARTDATE")
      (FZQDM, FBZ, FSH, FSTARTDATE)
    }).collect()
  }


  /**
    * zqdm 是维护的指数股票：
    * select 1 from A117CsTsKm where fstartdate<=日期 and fsh=1 and fbz=3 and fzqdm=该zqdm
    *
    * zqdm 是维护的指标股票：
    * select 1 from A117CsTsKm where fstartdate<=日期 and fsh=1 and fbz=2 and fzqdm=该zqdm
    */
  private def queryZhiShuOrZhiBiao(zqdm: String, fbz: String = "3", ywrq: String, defaultValue: Boolean = false): Boolean = {
    if (CSSYSTSKM_data == null || CSSYSTSKM_data.isEmpty) return defaultValue
    var value = defaultValue
    val break = new Breaks
    val ywrqLong = DateUtils.formattedDate2Long(ywrq, DateUtils.YYYYMMDD)
    break.breakable({
      for (item <- CSSYSTSKM_data) {
        val fstartdate = DateUtils.formattedDate2Long(item._4, DateUtils.YYYY_MM_DD)
        if (zqdm.equals(item._1) && fbz.equals(item._2) && "1".equals(item._3) && fstartdate <= ywrqLong) {
          value = true
          break.break()
        }
      }
    })
    value
  }


  private def readCSJYLV(spark: SparkSession): Unit = {
    CSJYLV_data = BasicUtils.readCSV(getTableDataPath("CSJYLV"), spark, header = false, sep = ",").toDF(
      "FZQLB",
      "FSZSH",
      "FFVLB",
      "FLV",
      "FJE",
      "FZKLV",
      "FOTHER",
      "FSH",
      "FZZR",
      "FCHK",
      "FJJDM",
      "FXWGD",
      "FBZ",
      "FSTARTDATE",
      "FISZKJZ",
      "FZKLJZRQ",
      "FHYDM",
      "FGDSXF",
      "FSXFLX",
      "FJSZX",
      "FJXTS",
      "IP/DB"
    ).rdd.map(row => {
      val flv = RowUtils.getRowFieldAsString(row, "FLV", "0")
      val fzqlb = RowUtils.getRowFieldAsString(row, "FZQLB")
      val ffvlb = RowUtils.getRowFieldAsString(row, "FFVLB")
      val fszsh = RowUtils.getRowFieldAsString(row, "FSZSH")
      val fjjdm = RowUtils.getRowFieldAsString(row, "FJJDM")
      (flv, fzqlb, ffvlb, fszsh, fjjdm)
    }).collect()

  }

  private def queryFlvFromCSJYLVByTzh(tzh: String, fvlb: String, defaultValue: String = "0"): String = {
    if (CSJYLV_data == null || CSJYLV_data.isEmpty) return defaultValue
    val break = new Breaks
    var value = defaultValue
    break.breakable({
      for (item <- CSJYLV_data) {
        val fzqlb = item._2
        val ffvlb = item._3
        val fszsh = item._4
        val fjjdm = item._5
        if ("KZZ".equals(fzqlb) && fvlb.equals(ffvlb) && "H".equals(fszsh) && tzh.equals(fjjdm)) {
          value = item._1
          break.break
        }
      }
    }
    )
    value
  }

  /**
    * 读取资产表,用于获取资产id FSETID
    *
    * @param spark SparkSession
    */
  private def readLSETLIST(spark: SparkSession): Unit = {
    fsetcode2Fsetid = BasicUtils.readCSV(getTableDataPath("LSETLIST"), spark, header = false, sep = ",").toDF(
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
      "FTZZHDM",
      "IP/DB"
    ).rdd.map(row => {
      val fsetcode = RowUtils.getRowFieldAsString(row, "FSETCODE")
      val fsetid = RowUtils.getRowFieldAsString(row, "FSETID")
      (fsetcode, fsetid)
    }).collect().toMap
  }

  /**
    * 根据套账号查询资产id
    *
    * @param fsetcode     套账号
    * @param defaultValue 默认值
    * @return
    */
  private def queryFsetIDByFsetcode(fsetcode: String, defaultValue: String = ""): String = {
    if (fsetcode2Fsetid == null || fsetcode2Fsetid.isEmpty) {
      return defaultValue
    }
    fsetcode2Fsetid.getOrElse(fsetcode, defaultValue)
  }


  /**
    * 读取股东代码表,用于获取席位号
    *
    * @param spark SparkSession
    */
  private def readCSGDZH(spark: SparkSession): Unit = {
    fgddm2Fsetcode = BasicUtils.readCSV(getTableDataPath("CSGDZH"), spark, header = false, sep = ",").toDF(
      "FGDDM",
      "FGDXM",
      "FSZSH",
      "FSH",
      "FZZR",
      "FSETCODE",
      "FCHK",
      "FSTARTDATE",
      "FACCOUNTTYPT",
      "IP/DB"
    ).rdd.map(row => {
      val fgddm = RowUtils.getRowFieldAsString(row, "FGDDM")
      val fsetcode = RowUtils.getRowFieldAsString(row, "FSETCODE")
      (fgddm, fsetcode)
    }).collect().toMap
  }


  /**
    * 根据套账号查询股东代码表,返回
    *
    * @param fgddm        股东代码
    * @param defaultValue 默认值
    * @return
    */
  private def queryFsetcodeByFgddm(fgddm: String, defaultValue: String = ""): String = {
    if (fgddm2Fsetcode == null || fgddm2Fsetcode.isEmpty) {
      return defaultValue
    }
    fgddm2Fsetcode.getOrElse(fgddm, defaultValue)
  }

  /**
    * cskzzhs where fzqdm=“zqdm” and fsh=1 and fstartdate<=业务日期 and fedate>=业务日期 and fbdate<=业务日期
    *
    * @param spark SparkSession
    * @return
    */

  private def readCSKZZHS(spark: SparkSession): Unit = {
    CSKZZHS_data = BasicUtils.readCSV(getTableDataPath("CSKZZHS"), spark, header = false, sep = ",").toDF(
      "FZQDM",
      "FHSJG",
      "FBDATE",
      "FEDATE",
      "FDZDATE",
      "FSH",
      "FZZR",
      "FCHK",
      "FSZSH",
      "FSTARTDATE",
      "IP/DB"
    ).rdd.map(row => {
      val fzqdm = RowUtils.getRowFieldAsString(row, "FZQDM")
      val fsh = RowUtils.getRowFieldAsString(row, "FSH")
      val fstartdate = RowUtils.getRowFieldAsString(row, "FSTARTDATE")
      val fedate = RowUtils.getRowFieldAsString(row, "FEDATE")
      val fbdate = RowUtils.getRowFieldAsString(row, "FBDATE")
      val fhsjg = RowUtils.getRowFieldAsString(row, "FHSJG", "0")
      (fzqdm, fsh, fstartdate, fedate, fbdate, fhsjg)
    }).collect()
  }


  /**
    * fzqdm=“zqdm” and fsh=1 and fstartdate<=业务日期 and fedate>=业务日期 and fbdate<=业务日期
    *
    * @return
    */
  private def queryCSKZZHSInfo(fzqdm: String, ywrq: String): Boolean = {

    var value = false
    if (CSKZZHS_data == null || CSKZZHS_data.isEmpty) return value

    val ywrqLong = DateUtils.formattedDate2Long(ywrq, DateUtils.YYYYMMDD)
    val break = new Breaks

    break.breakable(
      for (item <- CSKZZHS_data) {
        val zqdm = item._1
        val fsh = item._2
        val fstartdate = DateUtils.formattedDate2Long(item._3, DateUtils.YYYY_MM_DD)
        val fedate = DateUtils.formattedDate2Long(item._4, DateUtils.YYYY_MM_DD)
        val fbdate = DateUtils.formattedDate2Long(item._5, DateUtils.YYYY_MM_DD)
        if (zqdm.equals(fzqdm) && "1".equals(fsh) && fstartdate <= ywrqLong && fedate >= ywrqLong && fbdate <= ywrqLong) {
          value = true
          break.break
        }
      }

    )
    value
  }


  /**
    *
    * @param zqdm         证券代码
    * @param ywrq         业务日期
    * @param defaultValue 默认值
    * @return
    */
  private def queryFhsjgByZqdmAndYwrq(zqdm: String, ywrq: String, defaultValue: String = "0"): String = {
    if (CSKZZHS_data == null || CSKZZHS_data.isEmpty) return defaultValue
    val break = new Breaks
    val ywrqLone = DateUtils.formattedDate2Long(ywrq, DateUtils.YYYYMMDD)
    var value = defaultValue
    break.breakable({
      for (item <- CSKZZHS_data) {
        val fzqdm = item._1
        val fedate = DateUtils.formattedDate2Long(item._4, DateUtils.YYYY_MM_DD)
        val fbdate = DateUtils.formattedDate2Long(item._5, DateUtils.YYYY_MM_DD)

        if (zqdm.equals(fzqdm) && fedate >= ywrqLone && fbdate <= ywrqLone) {
          value = item._6
          break.break()
        }
      }
    })
    value

  }

  private def readCSZQXX(spark: SparkSession): Unit = {
    zqdm2Fscdm = BasicUtils.readCSV(getTableDataPath("CSZQXX"), spark, header = false, sep = ",").toDF(
      "FZQDM",
      "FZQMC",
      "FJJDM",
      "FJXQSR",
      "FJXJZR",
      "FSQPMLV",
      "FPMLV",
      "FPMJE",
      "FFXJG",
      "FFXCS",
      "FFXFS",
      "FZQLB",
      "FJYSC",
      "FSSDD",
      "FSCDM",
      "FLVLX",
      "FCXQSR",
      "FCXJZR",
      "FAN",
      "FSH",
      "FZZR",
      "FCHK",
      "FSTARTDATE",
      "FQYXX",
      "FJXFS",
      "FID",
      "FBZ",
      "FTYPE",
      "FENDFAN",
      "FJSJG",
      "FCBGZ",
      "FRLVJX",
      "FKXQRQ",
      "FFXR",
      "FTSFXR",
      "FHFXR",
      "FUPDATE",
      "IP/DB"
    ).rdd.map(row => {
      val zqdm = RowUtils.getRowFieldAsString(row, "FZQDM")
      val FSCDM = RowUtils.getRowFieldAsString(row, "FSCDM")
      (zqdm, FSCDM)
    }).collect().toMap
  }

  private def queryFscdmByFZQDM(fzqdm: String, defaultValue: String = ""): String = {
    if (zqdm2Fscdm == null || zqdm2Fscdm.isEmpty) {
      return defaultValue
    }
    zqdm2Fscdm.getOrElse(fzqdm, defaultValue)
  }


  /**
    * 读取参数表
    *
    * @param spark SparkSession
    */
  private def readLVARLIST(spark: SparkSession): Unit = {
    selectKey2Value = BasicUtils.readCSV(getTableDataPath("LVARLIST"), spark, header = false, sep = ",").toDF(
      "FVARNAME",
      "FVARVALUE",
      "FSH",
      "FZZR",
      "FCHK",
      "FSTARTDATE",
      "IP/DB"
    ).rdd.map(row => {
      val name = RowUtils.getRowFieldAsString(row, "FVARNAME")
      val value = RowUtils.getRowFieldAsString(row, "FVARVALUE")
      (name, value)
    }).collect().toMap

  }


  /**
    * 根据选项参数名查询选项是否选中
    *
    * @param varName      变量名
    * @param defaultValue 选项结果
    * @return
    */
  private def queryVarValueByVarName(varName: String, defaultValue: String = "0"): String = {
    if (selectKey2Value == null || selectKey2Value.isEmpty) return defaultValue
    selectKey2Value.getOrElse(varName, defaultValue)
  }


  /**
    * 读取CSQYXX 表
    *
    * @param spark SparkSession
    */
  private def readCSQYXX(spark: SparkSession): Unit = {
    CSQYXX_data = BasicUtils.readCSV(getTableDataPath("CSQYXX"), spark, header = false, sep = ",").toDF(
      "FZQDM",
      "FQYLX",
      "FQYBL",
      "FQYJG",
      "FQYDJR",
      "FQYCQR",
      "FJKJZR",
      "FSH",
      "FZZR",
      "FCHK",
      "FSZSH",
      "FSTARTDATE",
      "IP/DB"
    ).rdd.map(row => {
      val FZQDM = RowUtils.getRowFieldAsString(row, "FZQDM")
      val FQYLX = RowUtils.getRowFieldAsString(row, "FQYLX")
      val FQYBL = RowUtils.getRowFieldAsString(row, "FQYBL")
      val FQYJG = RowUtils.getRowFieldAsString(row, "FQYJG")
      val FQYDJR = RowUtils.getRowFieldAsString(row, "FQYDJR")
      val FQYCQR = RowUtils.getRowFieldAsString(row, "FQYCQR")
      val FJKJZR = RowUtils.getRowFieldAsString(row, "FJKJZR")
      (FZQDM, FQYLX, FQYBL, FQYJG, FQYDJR, FQYCQR, FJKJZR)
    }).collect()
  }


  /**
    * 现金对价：
    * select 1 from csqyxx where
    * FZqDm =zqdm and fqylx='XJDJ'and
    * fqydjr<=业务日期 and fjkjzr>=业务日期
    *
    * 股份对价：
    * select 1 from csqyxx where
    * FZqDm =zqdm and fqylx='GFDJ'and
    * fqydjr<=业务日期 and fjkjzr>=业务日期
    *
    * @param zqdm         证券代码
    * @param qylx         qy类别
    * @param ywrq         业务日期
    * @param defaultValue 默认值
    * @return
    */
  private def queryIsXjdjOrGpdj(zqdm: String, qylx: String, ywrq: String, defaultValue: Boolean = false): Boolean = {
    if (CSQYXX_data == null || CSQYXX_data.isEmpty) return defaultValue
    val break = new Breaks
    var value = defaultValue
    val ywrqLong = DateUtils.formattedDate2Long(ywrq, DateUtils.YYYYMMDD)
    break.breakable({
      for (item <- CSQYXX_data) {
        val fqydjr = DateUtils.formattedDate2Long(item._5, DateUtils.YYYY_MM_DD)
        val fjkjzr = DateUtils.formattedDate2Long(item._7, DateUtils.YYYY_MM_DD)
        if (zqdm.equals(item._1) && qylx.equals(item._2) && fqydjr <= ywrqLong && fjkjzr >= ywrqLong) {
          value = true
          break.break()
        }
      }
    })
    value
  }

  /**
    *
    * @param zqlb 证券类别
    * @param ywrq 业务日期
    * @param tzh  套账号
    * @return
    */
  private def queryPxJg(zqdm: String, zqlb: String, ywrq: String, tzh: String, defaultValue: String = "0"): String = {
    if (CSQYXX_data == null || CSQYXX_data.isEmpty) return defaultValue

    val break = new Breaks

    var fqyjg: String = null
    var fqybl: String = null

    val ywrqLong = DateUtils.formattedDate2Long(ywrq, DateUtils.YYYYMMDD)

    var FQYLXTemp: String = null

    if ("JJ".equals(zqlb)) {
      break.breakable(
        {
          for (item <- CSQYXX_data) {
            val FZQDM = item._1
            val FQYLX = item._2
            val FQYBL = item._3
            val FQYJG = item._4
            val fqydjr = DateUtils.formattedDate2Long(item._5, DateUtils.YYYY_MM_DD)
            val fjkjzr = DateUtils.formattedDate2Long(item._7, DateUtils.YYYY_MM_DD)
            if ("JJPX".equals(FQYLX) && FZQDM.equals(zqdm) && fqydjr <= ywrqLong && fjkjzr >= ywrqLong && (
              !"银行间".equals(FQYBL) && !"上交所".equals(FQYBL) && !"深交所".equals(FQYBL) && !"场外".equals(FQYBL)
              )) {
              fqyjg = FQYJG
              fqybl = FQYBL
              FQYLXTemp = FQYLX
              break.break
            }

          }

          if (fqyjg == null && fqybl == null) {
            for (item <- CSQYXX_data) {
              val FZQDM = item._1
              val FQYLX = item._2
              val FQYBL = item._3
              val FQYJG = item._4
              val fqydjr = DateUtils.formattedDate2Long(item._5, DateUtils.YYYY_MM_DD)
              val fjkjzr = DateUtils.formattedDate2Long(item._7, DateUtils.YYYY_MM_DD)


              if ("XJDJ".equals(FQYLX) && FZQDM.equals(zqdm) && fqydjr <= ywrqLong && fjkjzr >= ywrqLong && (
                !"银行间".equals(FQYBL) && !"上交所".equals(FQYBL) && !"深交所".equals(FQYBL) && !"场外".equals(FQYBL)
                )) {
                fqyjg = FQYJG
                fqybl = FQYBL
                FQYLXTemp = FQYLX
                break.break
              }

            }
          }

        }
      )
    } else {
      break.breakable(
        {
          for (item <- CSQYXX_data) {
            val FZQDM = item._1
            val FQYLX = item._2
            val FQYBL = item._3
            val FQYJG = item._4
            val fqydjr = DateUtils.formattedDate2Long(item._5, DateUtils.YYYY_MM_DD)
            val fjkjzr = DateUtils.formattedDate2Long(item._7, DateUtils.YYYY_MM_DD)


            if ("GPPX".equals(FQYLX) && FZQDM.equals(zqdm) && fqydjr <= ywrqLong && fjkjzr >= ywrqLong && (
              !"银行间".equals(FQYBL) && !"上交所".equals(FQYBL) && !"深交所".equals(FQYBL) && !"场外".equals(FQYBL)
              )) {
              fqyjg = FQYJG
              fqybl = FQYBL
              FQYLXTemp = FQYLX
              break.break
            }

          }

          if (fqyjg == null && fqybl == null) {
            for (item <- CSQYXX_data) {
              val FZQDM = item._1
              val FQYLX = item._2
              val FQYBL = item._3
              val FQYJG = item._4
              val fqydjr = DateUtils.formattedDate2Long(item._5, DateUtils.YYYY_MM_DD)
              val fjkjzr = DateUtils.formattedDate2Long(item._7, DateUtils.YYYY_MM_DD)


              if ("XJDJ".equals(FQYLX) && FZQDM.equals(zqdm) && fqydjr <= ywrqLong && fjkjzr >= ywrqLong && (
                !"银行间".equals(FQYBL) && !"上交所".equals(FQYBL) && !"深交所".equals(FQYBL) && !"场外".equals(FQYBL)
                )) {
                fqyjg = FQYJG
                fqybl = FQYBL
                FQYLXTemp = FQYLX
                break.break
              }

            }
          }

        }
      )
    }

    if (fqyjg == null || fqyjg == "") {
      fqyjg = defaultValue
    }

    if (fqybl == null || fqybl == "") {
      fqybl = defaultValue
    }

    if ("JJPX".equals(FQYLXTemp)) {
      val selectValue: String = queryVarValueByVarName(tzh + "股票分红按税前利率计算")
      if ("1".equals(selectValue)) {
        fqyjg
      } else {
        fqybl
      }
    } else {
      val selectValue: String = queryVarValueByVarName(tzh + "基金分红按税前利率计算")
      if ("1".equals(selectValue)) {
        fqyjg
      } else {
        fqybl
      }
    }
  }


  private def readJJGZLX(spark: SparkSession): Unit = {
    JJGZLX_data = BasicUtils.readCSV(getTableDataPath("JJGZLX"), spark, header = false, sep = ",").toDF(
      "FGZDM",
      "FJXRQ",
      "FYJLX",
      "FLXTS",
      "FPMLL",
      "FSZSH",
      "IP/DB"
    ).rdd.map(row => {
      val FYJLX = RowUtils.getRowFieldAsString(row, "FYJLX", "0")
      val FGZDM = RowUtils.getRowFieldAsString(row, "FGZDM")
      val FJXRQ = RowUtils.getRowFieldAsString(row, "FJXRQ")
      (FYJLX, FGZDM, FJXRQ)
    }).collect()
  }


  private def queryFYJLXByZqdmAndYwrq(zqdm: String, ywrq: String, defaultValue: String = "0"): String = {
    val break = new Breaks
    var value: String = null
    break.breakable({
      for (item <- JJGZLX_data) {
        val FGZDM = item._2
        val FJXRQ = item._3
        if (zqdm.equals(FGZDM) && ywrq.equals(FJXRQ)) {
          value = item._1
          break.break()
        }
      }
    })
    if (value == null)
      value = defaultValue
    value
  }


  /**
    * 进行计算
    *
    * @param spark    SparkSession
    * @param zqbdData RDD[Row]
    */
  private def caculate(spark: SparkSession, zqbdData: RDD[Row], ywrq: String): Unit = {

    // 过滤出分级基金的源数据
    val row151Data: RDD[Row] = zqbdData.filter(row => {
      val fywrq = RowUtils.getRowFieldAsString(row, "BDRQ")
      val bdlx = RowUtils.getRowFieldAsString(row, "BDLX")
      ywrq.equals(fywrq) && "151".equals(bdlx)
    })

    // 取差集得到非分级基金的结果数据
    val rowNot151Data: RDD[Row] = zqbdData.subtract(row151Data)

    // 非分级基金结果数据
    val not151ResData: RDD[Hzjkqs] = caculateNot151(spark, rowNot151Data, ywrq)

    // 分级基金结果数据
    val I51ResData: RDD[Hzjkqs] = caculate151(spark, row151Data, ywrq)

    // 将结果数据合并
    val shZQBDData: RDD[Hzjkqs] = not151ResData.union(I51ResData)

    //存hdfs
    saveToHDFS(spark, shZQBDData)
    //存mysql
    saveToMySQL(spark, shZQBDData)
  }


  /**
    * 将结果数据保存到HDFS
    *
    * @param spark      SparkSession
    * @param shZQBDData 结果数据
    */
  private def saveToHDFS(spark: SparkSession, shZQBDData: RDD[Hzjkqs]): Unit = {
    val date = DateUtils.formatDate(System.currentTimeMillis())
    val path = RES_HDFS_PATH + date + File.separator + "zqdb/"

    // 如果已经存在路径,则删除
    /*val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val hpath = new Path(path)
    if (fs.exists(hpath)) {
      fs.delete(hpath, true)
    }*/

    /*import spark.implicits._
    shZQBDData
      .toDF()
      .write
      .format("csv")
      .option("header", value = false)
      .option("delimiter", "\t")
      .option("charset", "UTF-8")
      .csv(path)*/
    import spark.implicits._
    BasicUtils.outputHdfs(shZQBDData.toDF(), path)

  }

  /**
    * 将结果数据保存到MySQL
    *
    * @param spark      SparkSession
    * @param shZQBDData 结果数据
    */
  private def saveToMySQL(spark: SparkSession, shZQBDData: RDD[Hzjkqs]): Unit = {
    import spark.implicits._
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "root1234")
    properties.put("driver", "com.mysql.jdbc.Driver")
    val url = "jdbc:mysql://192.168.102.120:3306/JJCWGZ"
    shZQBDData.toDF().write.mode(SaveMode.Overwrite).jdbc(url, "ShZQBD", properties)
  }


  /**
    * 计算非分级基金配对转换
    *
    * @param spark         SparkSession
    * @param rowNot151Data 非151的数据
    * @param ywrq          业务日期
    * @return
    */
  private def caculateNot151(spark: SparkSession, rowNot151Data: RDD[Row], ywrq: String): RDD[Hzjkqs] = {
    val calRes: RDD[(String, String)] = rowNot151Data.map(row => {
      val bdlx = RowUtils.getRowFieldAsString(row, "BDLX")
      val bdsl = BigDecimal(RowUtils.getRowFieldAsString(row, "BDSL", "0"))
      val qylb = RowUtils.getRowFieldAsString(row, "QYLB")
      val zqlb = RowUtils.getRowFieldAsString(row, "ZQLB")
      val ltlx = RowUtils.getRowFieldAsString(row, "LTLX")

      //股东代码
      val fgddm = RowUtils.getRowFieldAsString(row, "ZQZH")
      //根据股东代码获取套账号
      val fsetcode = queryFsetcodeByFgddm(fgddm)
      //根据套账号查询资产表,获取fsetid
      val FSETID = queryFsetIDByFsetcode(fsetcode)

      var Fdate = ywrq

      val FZqdm = RowUtils.getRowFieldAsString(row, "ZQDM")

      var FSzsh = "H"


      val Fjyxwh = getResultXWH(RowUtils.getRowFieldAsString(row, "XWH"))

      var FBS = "B"

      var Fje = BigDecimal("0.00")

      var Fsl = BigDecimal(0)

      val Fyj = BigDecimal(0)

      var Fjsf = BigDecimal(0)

      val Fyhs = BigDecimal(0)
      var Fzgf = BigDecimal(0)

      val Fghf = BigDecimal(0)
      val Fgzlx = BigDecimal(0)
      val Fhggain = BigDecimal(0)
      val Ffxj = BigDecimal(0)
      var Fsssje = BigDecimal(0)

      var FZqbz = ""
      var Fywbz = ""


      val FQsbz = "N"
      val FQTF = BigDecimal(0)
      val ZQDM = FZqdm
      val FJYFS = "PT"
      val Fsh = "1"
      val FZZR = " "
      val FCHK = " "
      val fzlh = "0"
      val ftzbz = " "
      val FQsghf = BigDecimal(0)

      val Fjybz = " "
      val ISRTGS = "1"
      val FPARTID = " "
      val FHTXH = " "
      val FCSHTXH = " "
      val FRZLV = BigDecimal(0)
      val FCSGHQX = BigDecimal(0)
      val FSJLY = " "
      val Fbz = "RMB"

      if ("00G".equals(bdlx) && bdsl > 0) {
        Fdate = NEXT_WORK_DAY
        if (FZqdm.startsWith("60")) {
          Fsl = bdsl
        } else {
          Fsl = bdsl / 100
        }

        if (FZqdm.startsWith("60")) {
          FZqbz = "XGLT"
          if ("PS".equals(zqlb)) {
            //2 指标
            Fywbz = "ZF"
            if (queryZhiShuOrZhiBiao(FZqdm, "2", ywrq)) {
              Fywbz = "ZBZF"
            }
            //3 指数
            if (queryZhiShuOrZhiBiao(FZqdm, "3", ywrq)) {
              Fywbz = "ZSZF"
            }

          } else {
            Fywbz = "PT"
            //2 指标
            if (queryZhiShuOrZhiBiao(FZqdm, "2", ywrq)) {
              Fywbz = "ZB"
            }
            //3 指数
            if (queryZhiShuOrZhiBiao(FZqdm, "3", ywrq)) {
              Fywbz = "ZS"
            }
          }

        } else if (FZqdm.startsWith("0")) {
          FZqbz = "XZLT"
          Fywbz = "GZXQ"
        } else if (FZqdm.startsWith("1")) {
          FZqbz = "XZLT"
          Fywbz = "QYZQ"
          if (FZqdm.startsWith("11") || FZqdm.startsWith("10")) {
            Fywbz = "KZZ"
          }

          if (FZqdm.startsWith("121")) {
            Fywbz = "ZCZQ"
          }

          if (FZqdm matches "^123[0-4][0-9]{2}.*") {
            Fywbz = "CJZQ"
          }
        }

      } else if ("HL".equals(qylb)) {
        Fdate = NEXT_WORK_DAY
        FBS = "S"
        Fje = bdsl * BigDecimal(queryPxJg(FZqdm, zqlb, ywrq, fsetcode))
        Fsssje = Fje
        FZqbz = "QY"
        if (queryPxJg(FZqdm, zqlb, ywrq, fsetcode, null) != null) {
          if (Fje != 0) {
            if ("00J".equals(bdlx)) {
              if (queryIsXjdjOrGpdj(FZqdm, "XJDJ", ywrq)) {
                Fywbz = "XJDJ"
              } else {
                Fywbz = "PX"
              }
            }

            if ("00K".equals(bdlx)) {
              if (queryIsXjdjOrGpdj(FZqdm, "XJDJ", ywrq)) {
                Fywbz = "XJDJDZ"
              } else {
                Fywbz = "PXDZ"
              }
            }

          }
        }
      } else if ("S".equals(qylb) || ("XL".equals(zqlb) && "F".equals(ltlx) && "00J".equals(bdlx))) {
        Fdate = NEXT_WORK_DAY
        Fsl = bdsl
        FZqbz = "QY"
        if ("S".equals(qylb)) {
          Fywbz = "SG"
          if (queryZhiShuOrZhiBiao(FZqdm, fbz = "3", ywrq) && queryIsXjdjOrGpdj(FZqdm, "GFDJ", ywrq)) {
            Fywbz = "ZSGFDJ"
          } else if (queryIsXjdjOrGpdj(FZqdm, "GFDJ", ywrq)) {
            Fywbz = "GFDJ"
          } else if (queryZhiShuOrZhiBiao(FZqdm, fbz = "3", ywrq)) {
            Fywbz = "ZSSG"
          }
        } else if ("XL".equals(zqlb) && "F".equals(ltlx) && "00J".equals(bdlx) && "" != qylb) {
          Fywbz = "XLSG"
          if (queryZhiShuOrZhiBiao(FZqdm, fbz = "3", ywrq) && queryIsXjdjOrGpdj(FZqdm, "GFDJ", ywrq)) {
            Fywbz = "ZSGFDJ"
          } else if (queryIsXjdjOrGpdj(FZqdm, "GFDJ", ywrq)) {
            Fywbz = "GFDJ"
          } else if (queryZhiShuOrZhiBiao(FZqdm, fbz = "3", ywrq)) {
            Fywbz = "ZSSG"
          }
        }

      } else if ("PZ".equals(zqlb) && "N".equals(ltlx) && "00J".equals(bdlx) && "".equals(qylb)) {
        Fdate = NEXT_WORK_DAY
        Fsl = bdsl.abs
        FZqbz = "QY"
        Fywbz = "QZ"

      } else if ("DX".equals(qylb)) {
        Fdate = NEXT_WORK_DAY
        FSzsh = queryFscdmByFZQDM(FZqdm)
        FBS = "S"
        Fsl = bdsl.abs * 100

        val bygzlx = BigDecimal(0) //TODO 百元国债利息 待做
        val dGzlx = queryFYJLXByZqdmAndYwrq(FZqdm, ywrq)

        Fje = Fsl * bygzlx


        if (!"0".equals(dGzlx)) {
          if ("1".equals(queryVarValueByVarName(fsetcode + "债券派息到账时派息金额按照税前利率计算"))) {
            Fje = Fsl * BigDecimal(dGzlx)
          }
        }

        Fsssje = Fje

        FZqbz = "QY"
        if ("00J".equals(bdlx)) {
          Fywbz = "ZQPX"
        } else {
          Fywbz = "PXDZ"
        }

      } else if ("PT".equals(zqlb) && "P".equals(qylb) && "00J".equals(bdlx)) {
        Fdate = ywrq
        Fsl = bdsl
        FZqbz = "QY"
        Fywbz = "KPSL"

      } else if ("00C".equals(bdlx) && queryCSKZZHSInfo(fgddm, ywrq)) {
        Fdate = ywrq
        FBS = "S"
        Fsl = bdsl.abs * 100

        Fje = Fsl * 100 * BigDecimal(queryFhsjgByZqdmAndYwrq(FZqdm, ywrq))
        Fjsf = Fje * BigDecimal(queryFlvFromCSJYLVByTzh(fsetcode, "JSF"))
        Fzgf = Fje * BigDecimal(queryFlvFromCSJYLVByTzh(fsetcode, "ZGF"))

        FZqbz = "ZQ"
        if (FZqdm.startsWith("1")) {
          if (FZqdm.startsWith("10") || FZqdm.startsWith("11")) {
            Fywbz = "KZZHS"
          } else if (FZqdm.startsWith("121")) {
            Fywbz = "ZCZQHS"
          } else {
            Fywbz = "QYZQHS"
          }
        }
        Fsssje = Fje + Fjsf + Fzgf

      } else if ("100".equals(bdlx) && "JJ".equals(zqlb) && "".equals(qylb)) {
        Fdate = RowUtils.getRowFieldAsString(row, "BDRQ")
        Fsl = bdsl
        FZqbz = "JJ"
        Fywbz = "SYJZ"
      }
      val FinDate = ywrq

      //结果根据FSETID,FDate,FInDate,FZqdm,FSzsh,FJyxwh,fzqbz，Fywbz，zqdm 列汇总

      val key = contactString("#", FSETID, Fdate, FinDate, FZqdm, FSzsh, Fjyxwh, FZqbz, Fywbz, ZQDM)

      val value = contactString("#",
        FSETID, //0
        Fdate, //1
        FinDate, //2
        FZqdm, //3
        FSzsh, //4
        Fjyxwh, //5
        FBS, //6
        Fje, //7
        Fsl, //8
        Fyj, //9
        Fjsf, //10
        Fyhs, //11
        Fzgf, //12
        Fghf, //13
        Ffxj, //14
        FQTF, //15
        Fgzlx, //16
        Fhggain, //17
        Fsssje, //18
        FZqbz, //19
        Fywbz, //20
        Fjybz, //21
        FQsbz, //22
        ZQDM, //23
        FJYFS, //24
        Fsh, //25
        FZZR, //26
        FCHK, //27
        fzlh, //28
        ftzbz, //29
        FQsghf, //30
        fgddm, //31
        ISRTGS, //32
        FPARTID, //33
        FHTXH, //34
        FCSHTXH, //35
        FRZLV, //36
        FCSGHQX, //37
        FSJLY, //38
        Fbz //39
      )

      (key, value)


    })

    calRes.groupByKey().map(item => {
      val it = item._2.iterator
      var FSETID: String = null //0
      var Fdate: String = null //1
      var FinDate: String = null //2
      var FZqdm: String = null //3
      var FSzsh: String = null //4
      var Fjyxwh: String = null //5
      var FBS: String = null //6
      var Fje = BigDecimal(0) //7
      var Fsl = BigDecimal(0) //8
      var Fyj = BigDecimal(0) //9
      var Fjsf = BigDecimal(0) //10
      var Fyhs = BigDecimal(0) //11
      var Fzgf = BigDecimal(0) //12
      var Fghf = BigDecimal(0) //13
      var Ffxj = BigDecimal(0) //14
      var FQTF = BigDecimal(0) //15
      var Fgzlx = BigDecimal(0) //16
      var Fhggain = BigDecimal(0) //17
      var Fsssje = BigDecimal(0) //18
      var FZqbz: String = null //19
      var Fywbz: String = null //20
      var Fjybz: String = null //21
      var FQsbz: String = null //22
      var ZQDM: String = null //23
      var FJYFS: String = null //24
      var Fsh: String = null //25
      var FZZR: String = null //26
      var FCHK: String = null //27
      var fzlh: String = null //28
      var ftzbz: String = null //28
      var FQsghf = BigDecimal(0) //30
      var fgddm: String = null //31
      var ISRTGS: String = null //32
      var FPARTID: String = null //33
      var FHTXH: String = null //34
      var FCSHTXH: String = null //35

      var FRZLV = BigDecimal(0) //36
      var FCSGHQX = BigDecimal(0) //37

      var FSJLY: String = null //38
      var Fbz: String = null //39

      for (item <- it) {
        val fields = item.split("#")
        if (FSETID == null) FSETID = fields(0)
        if (Fdate == null) Fdate = fields(1)
        if (FinDate == null) FinDate = fields(2)
        if (FZqdm == null) FZqdm = fields(3)
        if (FSzsh == null) FSzsh = fields(4)
        if (Fjyxwh == null) Fjyxwh = fields(5)
        if (FBS == null) FBS = fields(6)

        Fje += BigDecimal(fields(7))
        Fsl += BigDecimal(fields(8))
        Fyj += BigDecimal(fields(9))
        Fjsf += BigDecimal(fields(10))
        Fyhs += BigDecimal(fields(11))
        Fzgf += BigDecimal(fields(12))
        Fghf += BigDecimal(fields(13))
        Ffxj += BigDecimal(fields(14))
        FQTF += BigDecimal(fields(15))
        Fgzlx += BigDecimal(fields(16))
        Fhggain += BigDecimal(fields(17))
        Fsssje += BigDecimal(fields(18))

        if (FZqbz == null) FZqbz = fields(19)
        if (Fywbz == null) Fywbz = fields(20)
        if (Fjybz == null) Fjybz = fields(21)
        if (FQsbz == null) FQsbz = fields(22)
        if (ZQDM == null) ZQDM = fields(23)
        if (FJYFS == null) FJYFS = fields(24)
        if (Fsh == null) Fsh = fields(25)
        if (FZZR == null) FZZR = fields(26)
        if (FCHK == null) FCHK = fields(27)
        if (fzlh == null) fzlh = fields(28)
        if (ftzbz == null) ftzbz = fields(29)

        FQsghf += BigDecimal(fields(30))

        if (fgddm == null) fgddm = fields(31)
        if (ISRTGS == null) ISRTGS = fields(32)
        if (FPARTID == null) FPARTID = fields(33)
        if (FHTXH == null) FHTXH = fields(34)
        if (FCSHTXH == null) FCSHTXH = fields(35)

        FRZLV += BigDecimal(fields(36))
        FCSGHQX += BigDecimal(fields(37))

        if (FSJLY == null) FSJLY = fields(38)
        if (Fbz == null) Fbz = fields(39)
      }

      Hzjkqs(
        FSETID, //0
        DateUtils.changeDateForm(Fdate,DateUtils.YYYYMMDD,DateUtils.YYYY_MM_DD), //1
        DateUtils.changeDateForm(FinDate,DateUtils.YYYYMMDD,DateUtils.YYYY_MM_DD), //2
        FZqdm, //3
        FSzsh, //4
        Fjyxwh, //5
        FBS, //6
        Fje.abs.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString(), //7
        Fsl.toString(), //8
        Fyj.toString(), //9
        Fjsf.abs.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString(), //10
        Fyhs.abs.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString(), //11
        Fzgf.abs.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString(), //12
        Fghf.abs.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString(), //13
        Ffxj.abs.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString(), //14
        FQTF.abs.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString(), //15
        Fgzlx.abs.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString(), //16
        Fhggain.abs.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString(), //17
        Fsssje.abs.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString(), //18
        FZqbz, //19
        Fywbz, //20
        Fjybz, //21
        FQsbz, //22
        ZQDM, //23
        FJYFS, //24
        Fsh, //25
        FZZR, //26
        FCHK, //27
        fzlh, //28
        ftzbz, //29
        FQsghf.abs.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString(), //30
        fgddm, //31
        ISRTGS, //32
        FPARTID, //33
        FHTXH, //34
        FCSHTXH, //35
        FRZLV.abs.setScale(4, BigDecimal.RoundingMode.HALF_UP).toString(), //36
        FCSGHQX.abs.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString(), //37
        FSJLY, //38
        Fbz, //39
        FBY1 = "",
        FBY2 = "",
        FBY3 = "",
        FBY4 = "",
        FBY5 = ""
      )
    })
  }

  /**
    * 拼接字符串
    *
    * @param sep    拼接符
    * @param fields 要拼接得字符串
    * @return
    */
  private def contactString(sep: String, fields: AnyRef*): String = {
    if (fields.length < 0) return null
    val sb = new StringBuilder
    for (item <- fields) {
      sb.append(item).append(sep)
    }
    sb.delete(sb.length - 1, sb.length).toString()
  }


  /**
    * 计算分级基金配对转换
    *
    * @param spark      SparkSession
    * @param row151Data 151的数据
    * @param ywrq       业务日期
    * @return
    */
  private def caculate151(spark: SparkSession, row151Data: RDD[Row], ywrq: String): RDD[Hzjkqs] = {
    //分级基金配对转换
    import spark.implicits._
    row151Data.repartition(1).map(row => {
      ShZQBD(
        RowUtils.getRowFieldAsString(row, "SCDM"),
        RowUtils.getRowFieldAsString(row, "QSBH"),
        RowUtils.getRowFieldAsString(row, "ZQZH"),
        RowUtils.getRowFieldAsString(row, "XWH"),
        RowUtils.getRowFieldAsString(row, "ZQDM"),
        RowUtils.getRowFieldAsString(row, "ZQLB"),
        RowUtils.getRowFieldAsString(row, "LTLX"),
        RowUtils.getRowFieldAsString(row, "QYLB"),
        RowUtils.getRowFieldAsString(row, "GPNF"),
        RowUtils.getRowFieldAsString(row, "BDSL"),
        RowUtils.getRowFieldAsString(row, "BDLX"),
        RowUtils.getRowFieldAsString(row, "BDRQ"),
        RowUtils.getRowFieldAsString(row, "SL"),
        RowUtils.getRowFieldAsString(row, "BH"),
        RowUtils.getRowFieldAsString(row, "BY")
      )
    }).toDF().createTempView("t151")

    spark.sql(
      """
        |select t1.SCDM,t1.QSBH,t1.ZQZH,t1.XWH,t1.ZQDM,
        |t1.ZQLB,t1.LTLX,t1.QYLB,t1.GPNF,t1.BDSL,
        |t1.BDLX,t1.BDRQ,t1.SL,t1.BH,t1.BY,FLOOR((t1.INDEX-1) / 3) as PART
        |from
        |(
        |select *,row_number() OVER(partition by BDLX order by ZQDM) as INDEX from t151
        |) t1
        |""".stripMargin).rdd.map(row => {
      val part = row.getAs[Long]("PART") + ""
      (part, row)
    }).groupByKey()
      .map(tup => {
        val it = tup._2.toBuffer
        val len = it.size
        var zqdm: String = null
        if (len == 3) {
          var zCount = 0
          var fCount = 0
          // 1.第一次迭代可以判断母基金为负还是为正
          for (row <- it) {
            val sl = BigDecimal(RowUtils.getRowFieldAsString(row, "BDSL"))
            if (sl > 0) {
              zCount += 1
            } else {
              fCount += 1
            }
          }

          //母基金标志
          var m = 0
          if ((zCount + fCount) == 3) {
            if (zCount > fCount) {
              m = -1
            } else {
              m = 1
            }
          }

          //2.第二次迭代,得到母基金的zqdm
          val break = new Breaks
          break.breakable({
            for (row <- it) {
              val sl = BigDecimal(RowUtils.getRowFieldAsString(row, "BDSL"))
              val ZQDM = RowUtils.getRowFieldAsString(row, "ZQDM")
              if ((m > 0 && sl > 0) || (m < 0 && sl < 0)) {
                zqdm = ZQDM
                break.break()
              }
            }
          })

        }
        val list = ArrayBuffer[Hzjkqs]()

        //3. 第三次迭代,获取进行所有的逻辑计算
        for (row <- it) {
          //val bdlx = RowUtils.getRowFieldAsString(row, "BDLX")
          val bdsl = BigDecimal(RowUtils.getRowFieldAsString(row, "BDSL", "0"))
          //val qylb = RowUtils.getRowFieldAsString(row, "QYLB")
          //val zqlb = RowUtils.getRowFieldAsString(row, "ZQLB")
          //val ltlx = RowUtils.getRowFieldAsString(row, "LTLX")

          //股东代码
          val fgddm = RowUtils.getRowFieldAsString(row, "ZQZH")
          //根据股东代码获取套账号
          val fsetcode = queryFsetcodeByFgddm(fgddm)
          //根据套账号查询资产表,获取fsetid
          val FSETID = queryFsetIDByFsetcode(fsetcode)

          val Fdate = RowUtils.getRowFieldAsString(row, "BDRQ")
          val FinDate = Fdate

          val FZqdm = RowUtils.getRowFieldAsString(row, "ZQDM")

          val FSzsh = "H"
          val Fjyxwh = getResultXWH(RowUtils.getRowFieldAsString(row, "XWH"))

          var FBS = "B"
          if (bdsl <= 0) {
            FBS = "S"
          }

          val Fje = "0.00"

          val Fsl = bdsl

          val Fyj = "0.00"

          val Fjsf = "0.00"

          val Fyhs = "0.00"

          val Fzgf = "0.00"

          val Fghf = "0.00"

          val Fgzlx = "0.00"

          val Fhggain = "0.00"

          val Ffxj = "0.00"

          val Fsssje = "0.00"

          val FZqbz = "JJ"

          val Fywbz = "FJJJPDZH"

          val FQsbz = "N"

          val FQTF = "0.00"

          var ZQDM = FZqdm

          if (len == 3 && zqdm != null) {
            ZQDM = zqdm
          }


          val FJYFS = "PT"

          val Fsh = "1"

          val FZZR = " "

          val FCHK = " "

          val fzlh = "0"

          val ftzbz = " "

          val FQsghf = "0.00"

          val Fjybz = " "

          val ISRTGS = "1"

          val FPARTID = " "

          val FHTXH = " "

          val FCSHTXH = " "

          val FRZLV = "0.0000"

          val FCSGHQX = "0"

          val FSJLY = " "

          val Fbz = "RMB"

          val shZQBD = Hzjkqs(
            FSETID,
            DateUtils.changeDateForm(Fdate,DateUtils.YYYYMMDD,DateUtils.YYYY_MM_DD),
            DateUtils.changeDateForm(FinDate,DateUtils.YYYYMMDD,DateUtils.YYYY_MM_DD),
            FZqdm,
            FSzsh,
            Fjyxwh,
            FBS,
            Fje,
            Fsl.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString(),
            Fyj,
            Fjsf,
            Fyhs,
            Fzgf,
            Fghf,
            Ffxj,
            FQTF,
            Fgzlx,
            Fhggain,
            Fsssje,
            FZqbz,
            Fywbz,
            Fjybz,
            FQsbz,
            ZQDM,
            FJYFS,
            Fsh,
            FZZR,
            FCHK,
            fzlh,
            ftzbz,
            FQsghf,
            fgddm,
            ISRTGS,
            FPARTID,
            FHTXH,
            FCSHTXH,
            FRZLV,
            FCSGHQX,
            FSJLY,
            Fbz,
            FBY1 = "",
            FBY2 = "",
            FBY3 = "",
            FBY4 = "",
            FBY5 = ""
          )
          list.append(shZQBD)
        }
        list.toIterator
      }).flatMap(it => it.toList)
  }

  /**
    * fdate=业务日期 and (
    * (bdlx ='00G' and bdsl > 0)
    * or ((qylb in('HL','S','DX') and bdlx <>'00G')
    * or (zqlb='PZ' and ltlx='N' and bdlx ='00J' and trim(qylb)is null)
    * or (zqlb='XL' and ltlx='F' and bdlx ='00J' and trim(qylb)is null)
    * or (zqlb = 'PT' and qylb = 'P' and bdlx = '00J')
    * or (bdlx ='00C' and bdsl < 0)
    * or (bdlx ='100' and zqlb='JJ' and trim(qylb)is null))
    *
    * fdate=业务日期 and bdlx ='151'
    *
    * 过滤源数据
    *
    * @param zqdbOrgData RDD[Row]
    */
  private def filterData(zqdbOrgData: RDD[Row], ywrq: String) = {
    zqdbOrgData.filter(row => {
      val fdate = RowUtils.getRowFieldAsString(row, "BDRQ")
      val qylb: String = RowUtils.getRowFieldAsString(row, "QYLB")
      val bdlx: String = RowUtils.getRowFieldAsString(row, "BDLX")
      val bdsl: BigDecimal = BigDecimal(RowUtils.getRowFieldAsString(row, "BDSL", "0"))
      val ltlx: String = RowUtils.getRowFieldAsString(row, "LTLX")
      val zqlb: String = RowUtils.getRowFieldAsString(row, "ZQLB")

      val f1 = fdate.equals(ywrq)

      val f20 = "00G".equals(bdlx) || bdsl > 0
      val f21 = ("HL".equals(qylb) || "S".equals(qylb) || "DX".equals(qylb)) && !"00G".equals(bdlx)
      val f22 = "PZ".equals(zqlb) && "N".equals(ltlx) && "00J".equals(bdlx) && "".equals(qylb)
      val f23 = "XL".equals(zqlb) && "F".equals(ltlx) && "00J".equals(bdlx) && "".equals(qylb)
      val f24 = "PT".equals(zqlb) && "P".equals(qylb) && "00J".equals(bdlx)
      val f25 = "00C".equals(bdlx) && bdsl < 0
      val f26 = "100".equals(bdlx) && "JJ".equals(zqlb) && "".equals(qylb)
      val f27 = "151".equals(bdlx)
      f1 && (f20 || f21 || f22 || f23 || f24 || f25 || f26 || f27)
    })
  }


  /**
    * 读取证券变动的数据
    *
    * @param spark SparkSession
    */
  private def readZQBDData(spark: SparkSession, dataPath: String): RDD[Row] = {
    BasicUtils.readCSV(dataPath, spark).rdd
  }


  /**
    * 根据表名获取表在hdfs上对应的路径
    *
    * @param tName :表面
    * @return
    */
  private def getTableDataPath(tName: String): String = {
    val date = DateUtils.formatDate(System.currentTimeMillis())
    TABLE_HDFS_PATH + date + File.separator + tName
  }


  /**
    * 获取结果席位号:xwh取前5位，不足5位时前补0扩至5位
    *
    * @param xwh 从源数据中得到的席位号
    * @return
    */
  private def getResultXWH(xwh: String): String = {
    if (xwh == null) throw new IllegalArgumentException("席位号不能为null")
    val xwhLen = xwh.length
    if (xwhLen > 5) return xwh.substring(0, 5)
    "0" * (5 - xwhLen) + xwh
  }
}
