package com.yss.scala.core

import java.io.File

import com.yss.scala.util.{DateUtils, RowUtils, Util}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

import scala.util.control.Breaks

/**
  * @auther: lijiayan
  * @date: 2018/11/5
  * @desc: 上海证券变动
  */
object ShZQChange {

  val ZQBD_basePath = "hdfs://192.168.102.120:8020/yss/guzhi/interface/"

  //要使用的表在hdfs中的路径
  private val TABLE_HDFS_PATH = "hdfs://192.168.102.120:8020/yss/guzhi/basic_list/"

  //用于存储股东代码表中每一个股东代码对应的套账号
  var fgddm2Fsetcode: Map[String, String] = _

  //用于存储资产表中每一个套账号对应的资产id
  var fsetcode2Fsetid: Map[String, String] = _

  var CSKZZHS_data: Array[(String, String, String, String, String)] = _

  var zqdm2Fscdm: Map[String, String] = _

  var selectKey2Value: Map[String, String] = _

  var CSQYXX_data: RDD[(String, String, String, String, String, String, String)] = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    val day = DateUtils.formatDate(System.currentTimeMillis())
    var dataPath = ZQBD_basePath + day + "/zqbd.DBF.csv"
    var ywrq = "20160622"
    if (args != null && args.length == 2) {
      dataPath = args(0)
      ywrq = args(1)
    }

    val zqdbOrgData: RDD[Row] = readZQBDData(spark, dataPath)

    //将数据按照条件进行过滤
    val zqbdData: RDD[Row] = filterData(zqdbOrgData, ywrq)

    zqbdData.persist()

    //读取股东代码表
    readCSGDZH(spark)

    //读取资产表
    readLSETLIST(spark)

    //读取回售信息新表
    readCSKZZHS(spark)

    //读取债券信息表
    readCSZQXX(spark)

    //读取参数表
    readLVARLIST(spark)

    readCSQYXX(spark)

    //进行计算
    caculate(spark, zqbdData, ywrq)


    spark.stop()

  }

  /**
    * 读取资产表,用于获取资产id FSETID
    *
    * @param spark SparkSession
    */
  def readLSETLIST(spark: SparkSession) = {
    fsetcode2Fsetid = Util.readCSV(getTableDataPath("LSETLIST"), spark, header = false, sep = ",").toDF(
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
      "FZZR ",
      "FCHK",
      "FTZJC",
      "FZYDM",
      "FTZZHDM"
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
  def queryFsetIDByFsetcode(fsetcode: String, defaultValue: String = ""): String = {
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
  def readCSGDZH(spark: SparkSession) = {
    fgddm2Fsetcode = Util.readCSV(getTableDataPath("CSGDZH"), spark, header = false, sep = ",").toDF(
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
  def queryFsetcodeByFgddm(fgddm: String, defaultValue: String = ""): String = {
    if (fgddm2Fsetcode == null || fgddm2Fsetcode.isEmpty) {
      return defaultValue
    }
    fgddm2Fsetcode.getOrElse(fgddm, defaultValue)
  }

  /**
    * cskzzhs where fzqdm=“zqdm” and fsh=1 and fstartdate<=业务日期 and fedate>=业务日期 and fbdate<=业务日期
    *
    * @param spark
    * @return
    */

  def readCSKZZHS(spark: SparkSession): Unit = {
    CSKZZHS_data = Util.readCSV(getTableDataPath("CSKZZHS"), spark, header = false, sep = ",").toDF(
      "FZQDM",
      "FHSJG",
      "FBDATE",
      "FEDATE",
      "FDZDATE",
      "FSH",
      "FZZR",
      "FCHK",
      "FSZSH",
      "FSTARTDATE"
    ).rdd.map(row => {
      val fzqdm = RowUtils.getRowFieldAsString(row, "FZQDM")
      val fsh = RowUtils.getRowFieldAsString(row, "FSH")
      val fstartdate = RowUtils.getRowFieldAsString(row, "FSTARTDATE")
      val fedate = RowUtils.getRowFieldAsString(row, "FEDATE")
      val fbdate = RowUtils.getRowFieldAsString(row, "FBDATE")
      (fzqdm, fsh, fstartdate, fedate, fbdate)
    }).collect()
  }


  /**
    * fzqdm=“zqdm” and fsh=1 and fstartdate<=业务日期 and fedate>=业务日期 and fbdate<=业务日期
    *
    * @return
    */
  def queryCSKZZHSInfo(fzqdm: String, ywrq: String): Boolean = {

    var value = false
    if (CSKZZHS_data == null || CSKZZHS_data.isEmpty) return value

    val ywrqLong = DateUtils.formattedDate2Long(ywrq, DateUtils.YYYYMMDD)
    val break = new Breaks

    break.breakable(
      for (item <- CSKZZHS_data) {
        val zqdm = item._1
        val fsh = item._2
        val fstartdate = DateUtils.formattedDate2Long(item._3, DateUtils.YYYY_MM_DD_HH_MM_SS)
        val fedate = DateUtils.formattedDate2Long(item._4, DateUtils.YYYY_MM_DD_HH_MM_SS)
        val fbdate = DateUtils.formattedDate2Long(item._5, DateUtils.YYYY_MM_DD_HH_MM_SS)
        if (zqdm.equals(fzqdm) && "1".equals(fsh) && fstartdate <= ywrqLong && fedate >= ywrqLong && fbdate <= ywrqLong) {
          value = true
          break.break
        }
      }

    )
    value
  }


  def readCSZQXX(spark: SparkSession): Unit = {
    zqdm2Fscdm = Util.readCSV(getTableDataPath("CSZQXX"), spark, header = false, sep = ",").toDF(
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
      "FUPDATE"
    ).rdd.map(row => {
      val zqdm = RowUtils.getRowFieldAsString(row, "FZQDM")
      val FSCDM = RowUtils.getRowFieldAsString(row, "FSCDM")
      (zqdm, FSCDM)
    }).collect().toMap
  }

  def queryFscdmByFZQDM(fzqdm: String, defaultValue: String = ""): String = {
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
  def readLVARLIST(spark: SparkSession): Unit = {
    selectKey2Value = Util.readCSV(getTableDataPath("LVARLIST"), spark, header = false, sep = ",").toDF(
      "FVARNAME",
      "FVARVALUE",
      "FSH",
      "FZZR",
      "FCHK",
      "FSTARTDATE"
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
  def queryVarValueByVarName(varName: String, defaultValue: String = "0"): String = {
    if (selectKey2Value == null || selectKey2Value.isEmpty) return defaultValue
    selectKey2Value.getOrElse(varName, defaultValue)
  }


  /**
    * 读取CSQYXX 表
    *
    * @param spark SparkSession
    */
  def readCSQYXX(spark: SparkSession): Unit = {
    CSQYXX_data = Util.readCSV(getTableDataPath("CSQYXX"), spark, header = false, sep = ",").toDF(
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
      "FSTARTDATE"
    ).rdd.map(row => {
      val FZQDM = RowUtils.getRowFieldAsString(row, "FZQDM")
      val FQYLX = RowUtils.getRowFieldAsString(row, "FQYLX")
      val FQYBL = RowUtils.getRowFieldAsString(row, "FQYBL")
      val FQYJG = RowUtils.getRowFieldAsString(row, "FQYJG")
      val FQYDJR = RowUtils.getRowFieldAsString(row, "FQYDJR")
      val FQYCQR = RowUtils.getRowFieldAsString(row, "FQYCQR")
      val FJKJZR = RowUtils.getRowFieldAsString(row, "FJKJZR")
      (FZQDM, FQYLX, FQYBL, FQYJG, FQYDJR, FQYCQR, FJKJZR)
    })
  }

  /**
    *
    * @param zqlb 证券类别
    * @param ywrq 业务日期
    * @param tzh  套账号
    * @return
    */
  def queryPxJg(zqdm: String, zqlb: String, ywrq: String, tzh: String, defaultValue: String = "0"): String = {
    if (CSQYXX_data == null || CSQYXX_data.isEmpty()) return defaultValue

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
            val fqydjr = DateUtils.formattedDate2Long(item._5, DateUtils.YYYY_MM_DD_HH_MM_SS)
            val fjkjzr = DateUtils.formattedDate2Long(item._7, DateUtils.YYYY_MM_DD_HH_MM_SS)


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
              val fqydjr = DateUtils.formattedDate2Long(item._5, DateUtils.YYYY_MM_DD_HH_MM_SS)
              val fjkjzr = DateUtils.formattedDate2Long(item._7, DateUtils.YYYY_MM_DD_HH_MM_SS)


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
            val fqydjr = DateUtils.formattedDate2Long(item._5, DateUtils.YYYY_MM_DD_HH_MM_SS)
            val fjkjzr = DateUtils.formattedDate2Long(item._7, DateUtils.YYYY_MM_DD_HH_MM_SS)


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
              val fqydjr = DateUtils.formattedDate2Long(item._5, DateUtils.YYYY_MM_DD_HH_MM_SS)
              val fjkjzr = DateUtils.formattedDate2Long(item._7, DateUtils.YYYY_MM_DD_HH_MM_SS)


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


  /**
    * 进行计算
    *
    * @param spark    SparkSession
    * @param zqbdData RDD[Row]
    */
  private def caculate(spark: SparkSession, zqbdData: RDD[Row], ywrq: String) = {

    zqbdData.map(row => {
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
      val fsetID = queryFsetIDByFsetcode(fsetcode)

      var Fdate = ywrq

      val FZqdm = RowUtils.getRowFieldAsString(row, "ZQDM")

      var FSzsh = "H"


      val Fjyxwh = getResultXWH(RowUtils.getRowFieldAsString(row, "XWH"))

      var FBS = "B"

      var Fje = BigDecimal("0.00")

      var Fsl = BigDecimal(0)

      if ("00G".equals(bdlx) && bdsl > 0) {
        if (FZqdm.startsWith("60")) {
          Fsl = bdsl
        } else {
          Fsl = bdsl / 100
        }

      } else if ("HL".equals(qylb)) {
        FBS = "S"
        Fje = bdsl * BigDecimal(queryPxJg(FZqdm, zqlb, ywrq, fsetcode)).abs.setScale(2,BigDecimal.RoundingMode.HALF_UP)

      } else if ("S".equals(qylb) && "XL".equals(zqlb) && "F".equals(ltlx) && "00J".equals(bdlx)) {
        Fsl = bdsl

      } else if ("PZ".equals(zqlb) && "N".equals(ltlx) && "00J".equals(bdlx) && "".equals(qylb)) {
        Fsl = bdsl.abs

      } else if ("DX".equals(qylb)) {
        FSzsh = queryFscdmByFZQDM(FZqdm)
        FBS = "S"
        Fsl = bdsl.abs * 100

      } else if ("PT".equals(zqlb) && "P".equals(qylb) && "00J".equals(bdlx)) {
        Fsl = bdsl

      } else if ("00C".equals(bdlx) && queryCSKZZHSInfo(fgddm, ywrq)) {
        FBS = "S"
        Fsl = bdsl.abs * 100
      } else if ("100".equals(bdlx) && "JJ".equals(zqlb) && "".equals(qylb)) {
        Fdate = RowUtils.getRowFieldAsString(row, "BDRQ")
        Fsl = bdsl
      }
      val FinDate = Fdate
    })

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
    Util.readCSV(dataPath, spark, sep = ",").rdd
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


  /**
    * 获取结果席位号:xwh取前5位，不足5位时前补0扩至5位
    *
    * @param xwh 从源数据中得到的席位号
    * @return
    */
  def getResultXWH(xwh: String): String = {
    if (xwh == null) throw new IllegalArgumentException("席位号不能为null")
    val xwhLen = xwh.length
    if (xwhLen > 5) return xwh.substring(0, 5)
    "0" * (5 - xwhLen) + xwh
  }
}
