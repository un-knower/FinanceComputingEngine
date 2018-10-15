package com.yss.scala.guzhi

import com.yss.scala.dto.{Hzjkqs, ShghFee, ShghYssj}
import com.yss.scala.guzhi.ShghContants._
import com.yss.scala.util.{DateUtils, Util}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.math.BigDecimal.RoundingMode

/**
  * @author ws
  * @version 2018-08-30
  *          描述：上海过户动态获取参数
  *          源文件：gdh.dbf
  *          结果表：SHDZGH
  */
object SHTransfer {

  def main(args: Array[String]): Unit = {
    doExec(args(0),args(1))
  }

  def doExec(fileName:String,tableName:String) = {
    val spark = SparkSession.builder()
      .appName("SHTransfer")
      .master("local[*]")
      .getOrCreate()
    val broadcastLvarList = loadLvarlist(spark.sparkContext)
    val df = doETL(spark, broadcastLvarList,fileName)
    import spark.implicits._
    doSum(spark, df.toDF(), broadcastLvarList,tableName)
    spark.stop()
  }

  /** * 加载公共参数表lvarlist */
  def loadLvarlist(sc: SparkContext) = {

    val csbPath = Util.getDailyInputFilePath(TABLE_NAME_GGCS)
    val csb = sc.textFile(csbPath)

    //将参数表转换成map结构
    val csbMap = csb.map(row => {
      val fields = row.split(SEPARATE2)
      val key = fields(0)
      val value = fields(1)
      (key, value)
    })
      .collectAsMap()
    sc.broadcast(csbMap)
  }

  /** 加载各种基础信息表，并广播 */
  def loadTables(spark: SparkSession, today: String) = {

    val sc = spark.sparkContext

    /** * 读取基金信息表csjjxx */
    def loadCsjjxx() = {
      val csjjxxPath = Util.getDailyInputFilePath(TABLE_NAME_JJXX)
      val csjjxx = sc.textFile(csjjxxPath)
        .filter(row => {
          val fields = row.split(SEPARATE2)
          val fsh = fields(10)
          val fszsh = fields(8)
          if (FSH.equals(fsh) && SH.equals(fszsh)) true
          else false
        })
        .map(row => {
          val fields = row.split(SEPARATE2)
          val zqdm = fields(1) //证券代码
          val fzqlx = fields(9) //证券类型
          (zqdm + SEPARATE1 + fzqlx, row)
        })
        .groupByKey()
        .mapValues(rows => {
          //  fetftype='0'
          val filteredRows = rows.filter(str => str.split(",")(17).equals("0"))
          val ele1 =
            if (filteredRows.size == 0) DEFAULT_VALUE
            else filteredRows.toArray.sortWith((str1, str2) => {
              str1.split(SEPARATE2)(14).compareTo(str2.split(SEPARATE2)(14)) < 0
            })(0)

          (ele1, rows.toArray.sortWith((str1, str2) => {
            str1.split(SEPARATE2)(14).compareTo(str2.split(SEPARATE2)(14)) < 0
          })(0))
        })

      //基金信息(CsJjXx表)中维护的FZQDMETF1=该zqdm
      val csjjxx01Map = csjjxx.map(row => {
        val fields = row._2._2.split(SEPARATE2)
        val FZQLX = fields(9)
        val FZQDMETF0 = fields(3)
        val FSTARTDATE = fields(14)
        (FZQLX + SEPARATE1 + FZQDMETF0, FSTARTDATE)
      }).collectAsMap()
      val csjjxx02Map = csjjxx.map(row => {
        val fields = row._2._2.split(SEPARATE2)
        val FZQLX = fields(9)
        val FZQDMETF0 = fields(4)
        val FSTARTDATE = fields(14)
        (FZQLX + SEPARATE1 + FZQDMETF0, FSTARTDATE)
      }).collectAsMap()
      val csjjxx05Map = csjjxx.map(row => {
        val fields = row._2._2.split(SEPARATE2)
        val FZQLX = fields(9)
        val FZQDMETF0 = fields(16)
        val FSTARTDATE = fields(14)
        (FZQLX + SEPARATE1 + FZQDMETF0, FSTARTDATE)
      }).collectAsMap()
      val csjjxx03Map = csjjxx.map(row => {
        val fields = row._2._2.split(SEPARATE2)
        val FZQLX = fields(9)
        val FZQDMETF0 = fields(5)
        val FSTARTDATE = fields(14)
        (FZQLX + SEPARATE1 + FZQDMETF0, FSTARTDATE)
      }).collectAsMap()
      val csjjxx04Map = csjjxx.map(row => {
        val fields = row._2._2.split(SEPARATE2)
        val FZQLX = fields(9)
        val FZQDMETF0 = fields(6)
        val FSTARTDATE = fields(14)
        (FZQLX + SEPARATE1 + FZQDMETF0, FSTARTDATE)
      }).collectAsMap()
      val csjjxx00Map = csjjxx.map(row => {
        val fields = row._2._2.split(SEPARATE2)
        val FZQLX = fields(9)
        val FZQDMETF0 = fields(2)
        val FSTARTDATE = fields(14)
        (FZQLX + SEPARATE1 + FZQDMETF0, FSTARTDATE)
      }).collectAsMap()
      // 过滤 fetftype ='0' FZQLX='HB'的情况
      val csjjxx01HPMap = csjjxx.filter(
        row => {
          !row._2._1.equals(DEFAULT_VALUE)
        }
      ).map(row => {
        val fields = row._2._1.split(SEPARATE2)
        val FZQLX = fields(9)
        val FZQDMETF0 = fields(2)
        val FSTARTDATE = fields(14)
        (FZQLX + SEPARATE1 + FZQDMETF0, FSTARTDATE)
      }).collectAsMap()
      val csjjxxMap = Map((0, csjjxx00Map), (1, csjjxx01Map), (2, csjjxx02Map), (3, csjjxx03Map), (4, csjjxx04Map), (5, csjjxx05Map), (6, csjjxx01HPMap))
      sc.broadcast(csjjxxMap)
    }

    /** 加载权益信息表csqyxx， */
    def loadCsqyxx() = {
      val csqyxx = Util.getDailyInputFilePath(TABLE_NAME_QYXX)
      val csqyxMap = sc.textFile(csqyxx)
        .filter(row => {
          val fields = row.split(SEPARATE2)
          val fqylx = fields(1)
          val fsh = fields(7)
          if ("PG".equals(fqylx) && FSH.equals(fsh)) true
          else false
        })
        .map(row => {
          val fields = row.split(SEPARATE2)
          val fzqdm = fields(0)
          (fzqdm, row)
        })
        .groupByKey()
        .collectAsMap()
      sc.broadcast(csqyxMap)
    }

    /** 读取席位表CsqsXw表 */
    def loadCsqsxw() = {
      val csqsxwPath = Util.getDailyInputFilePath(TABLE_NAME_QSXW)
      val csqsxw = sc.textFile(csqsxwPath)
        .filter(row => {
          val fields = row.split(SEPARATE2)
          val fxwlb = fields(4)
          val fsh = fields(6)
          if (FSH.equals(fsh) && ("ZS".equals(fxwlb) || "ZYZS".equals(fxwlb))) true
          else false
        })
        .map(row => {
          val fields = row.split(SEPARATE2)
          val fstartdate = fields(9)
          val fqsxw = fields(3)
          val fsetcode = fields(5)
          (fsetcode + SEPARATE1 + fqsxw, fstartdate)
        })
        .groupByKey()
        .mapValues(rows => {
          rows.toArray.sortWith(_.compareTo(_) <= 0)(0)
        })
        .collectAsMap()
      sc.broadcast(csqsxw)
    }

    /** 读取 特殊科目设置表CsTsKm */
    def loadCsTsKm() = {
      val csTsKmPath = Util.getDailyInputFilePath(TABLE_NAME_TSKM)
      val csTsKm = sc.textFile(csTsKmPath)
        .map(row => {
          val fields = row.split(SEPARATE2)
          val fsh = fields(2)
          val fbz = fields(1)
          val fzqdm = fields(0)
          val startdate = fields(5)
          (fsh + SEPARATE1 + fbz + SEPARATE1 + fzqdm, startdate)
        })
        .groupByKey()
        .mapValues(rows => {
          rows.toArray.sortWith(_.compareTo(_) <= 0)(0)
        })
        .collectAsMap()
      sc.broadcast(csTsKm)
    }

    /** 获取基金类型 lsetcssysjj */
    def loadCssysjj() = {
      val lsetcssysjjPath = Util.getDailyInputFilePath(TABLE_NAME_SYSJJ)
      val lsetcssysjj = sc.textFile(lsetcssysjjPath)
      val lsetcssysjjMap = lsetcssysjj
        .map(row => {
          val fields = row.split(SEPARATE2)
          (fields(0), fields(1) + SEPARATE1 + fields(3))
        })
        .collectAsMap()
      sc.broadcast(lsetcssysjjMap)
    }

    /** 债券信息表cszqxx */
    def loadCszqxx() = {
      //读取CsZqXx表
      val cszqxxPath = Util.getDailyInputFilePath(TABLE_NAME_ZQXX)
      val cszqxx = sc.textFile(cszqxxPath)
      //则获取CsZqXx表中fzqdm=gh文件中的zqdm字段的“FZQLX”字段值
      val cszqxxMap1 = cszqxx
        .map(row => {
          val fields = row.split(SEPARATE2)
          val zqdm = fields(0)
          val fzqlx = fields(11)
          (zqdm, fzqlx)
        }).collectAsMap()

      val cszqxxMap2 = cszqxx.filter(row => {
        val fields = row.split(SEPARATE2)
        val zqdm = fields(0)
        val fzqlb = fields(11)
        val fjysc = fields(12)
        val fsh = fields(19)
        val fjjdm = fields(2)
        val fstartdate = fields(22)
        if (!"银行间".equals(fjysc) && "可分离债券".equals(fzqlb)
          && FSH.equals(fsh)) true
        else false
      })
        .map(row => {
          val fields = row.split(SEPARATE2)
          val zqdm = fields(0)
          val fzqlb = fields(11)
          val fjysc = fields(12)
          val fsh = fields(19)
          val fjjdm = fields(2)
          val fstartdate = fields(22)
          (zqdm + SEPARATE1 + fjjdm, fstartdate)
        })
        .groupByKey()
        .mapValues(rows => {
          rows.toArray.sortWith((str1, str2) => {
            str1.split(SEPARATE1)(0).compareTo(str2.split(SEPARATE1)(0)) < 0
          })(0)
        })
        .collectAsMap()
      (sc.broadcast(cszqxxMap1), sc.broadcast(cszqxxMap2))
    }

    /** 股东账号表csgdzh */
    def loadCsgdzh() = {
      //读取股东账号表，
      val csgdzhPath = Util.getDailyInputFilePath(TABLE_NAME_GDZH)
      val csgdzhMap = sc.textFile(csgdzhPath)
        .map(row => {
          val fields = row.split(SEPARATE2)
          (fields(0), fields(5))
        }).collectAsMap()

      sc.broadcast(csgdzhMap)
    }

    /** 已计提国债利息 JJGZLX */
    def loadGzlx() = {
      val jjgzlxPath = Util.getDailyInputFilePath(TABLE_NAME_GZLX)
      val jjgzlxMap = sc.textFile(jjgzlxPath)
        .map(str => {
          val fields = str.split(SEPARATE2)
          val fjxrq = fields(1)
          val fgzdm = fields(0)
          val fyjlx = fields(2)
          (fgzdm + SEPARATE1 + fjxrq, fyjlx)
        }).collectAsMap()
      sc.broadcast(jjgzlxMap)
    }

    /** 加载节假日表 csholiday */
    def loadCsholiday() = {
      val csholidayPath = Util.getDailyInputFilePath(TABLE_NAME_HOLIDAY)
      val csholidayList = sc.textFile(csholidayPath)
        .filter(str => {
          val fields = str.split(SEPARATE2)
          val fdate = fields(0)
          val fbz = fields(1)
          val fsh = fields(3)
          if ("0".equals(fbz) && FSH.equals(fsh) && fdate.compareTo(today) >= 0) true
          else false
        })
        .map(str => {
          str.split(SEPARATE2)(0)
        }).takeOrdered(1)
      if (csholidayList.length == 0) today
      else csholidayList(0)
    }

    (loadCsjjxx(), loadCsqyxx(), loadCsqsxw(),
      loadCsTsKm(), loadCssysjj(), loadCszqxx(), loadCsgdzh(), loadGzlx(), loadCsholiday())
  }

  /** 进行原始数据的转换包括：规则1，2，3，4，5 */
  def doETL(spark: SparkSession, csb: Broadcast[collection.Map[String, String]],fileName:String) = {
    val sc = spark.sparkContext

//    import com.yss.scala.dbf.dbf._
    val sourcePath = Util.getDailyInputFilePath(fileName,PREFIX)
    val df = Util.readCSV(sourcePath,spark)

//    val sourcePath ="C:\\Users\\wuson\\Desktop\\GuZhi\\test\\20180126\\gh.dbf"
//    val df = spark.sqlContext.dbfFile(sourcePath)
    val today = DateUtils.getToday(DateUtils.yyyy_MM_dd)
    // (larlistValue, csjjxxValue,csqyxxValue, csqsxwValue, csTsKmValue, lsetcssysjjValue, csqzxxValue, csgdzhValue)
    val broadcaseValues = loadTables(spark, today)
    val csbValues = csb
    val csjjxxValues = broadcaseValues._1
    val csqyxValues = broadcaseValues._2
    val csqsxwValue = broadcaseValues._3
    val csTsKmValue = broadcaseValues._4
    val lsetcssysjjValues = broadcaseValues._5
    val cszqxxValues = broadcaseValues._6
    val csgdzhValues = broadcaseValues._7
    val gzlxValues = broadcaseValues._8
    val csholiday = broadcaseValues._9

    /**
      * 计算国债利息
      * FZQBZ=ZQ时，已计提国债利息(如果为0则取税前百元国债利息)*文件中cjsl*10
      *
      * @param zqbz 证券标志
      * @param zqdm 证券代码
      * @param bcrq 日期
      * @param cjsl 成交数量
      * @return
      */
    def gzlx(zqbz: String, zqdm: String, bcrq: String, cjsl: String): String = {
      var gzlx = "0"
      if ("ZQ".equals(zqbz)) {
        val may = gzlxValues.value.get(zqdm + SEPARATE1 + bcrq)
        if (may.isDefined) {
          gzlx = BigDecimal(may.get).*(BigDecimal(cjsl)).*(BigDecimal(10)).setScale(DEFAULT_DIGIT, RoundingMode.HALF_UP).formatted(DEFAULT_DIGIT_FORMAT)
        }
      }
      gzlx
    }

    /**
      * 判断基金信息表维护 FZQDMETF0、1 、2、3、4、5=该zqdm
      * select 1 from csjjxx a,(select FZQDM, FZQLX,max(fstartdate) as fstartdate from csjjxx where fsh=1 and fstartdate<=日期 group by fzqdm,fzqlx) b where a.fsh=1 and a.fzqdm=b.fzqdm and a.fzqlx=b.fzqlx and a.fstartdate=b.fstartdate and a.FSZSH='H' and a.FZQLX='ETF' where a.FZQDMETF0=该zqdm
      *
      * @param fzqlx 基金类型ETF / HP
      * @param zqdm  证券代码
      * @param bcrq  日期
      * @param flag  FZQDMETF0、1 、2、3、4、5
      * @return 是否维护
      */
    def jjxxbwh(fzqlx: String, zqdm: String, bcrq: String, flag: Int): Boolean = {
      val map = csjjxxValues.value(flag)
      val maybeString = map.get(fzqlx + SEPARATE1 + zqdm)
      if (maybeString.isDefined) {
        if (bcrq.compareTo(maybeString.get) >= 0) return true
      }
      return false
    }

    /**
      * 判断是否是配股股票
      * select 1 from csqyxx where fqylx='PG' and fqydjr<=读数日期 and fjkjzr>=读数日期 and fstartdate<=读数日期 and fqybl not in('银行间','上交所','深交所','场外') and fsh=1 and fzqdm='gh文件中的zqdm'
      *
      * @param zqdm 证券代码
      * @param bcrq 读数日期
      * @return 是否
      */
    def sfspggp(zqdm: String, bcrq: String): Boolean = {
      val maybeRows = csqyxValues.value.get(zqdm)
      if (maybeRows.isDefined) {
        val condition1 = Array("银行间", "上交所", "深交所", "场外")
        for (row <- maybeRows.get) {
          val fields = row.split(SEPARATE2)
          val fqydjr = fields(4)
          val fjkjzr = fields(6)
          val fstartdate = fields(11)
          val fqybl = fields(2)

          if (bcrq.compareTo(fqydjr) >= 0
            && bcrq.compareTo(fjkjzr) <= 0
            && bcrq.compareTo(fstartdate) >= 0
            && !condition1.contains(fqybl)) return true
        }
      }
      return false
    }

    /**
      * 判断是否要查询CsZqXx中ZQ的业务类型
      *
      * @param tzh  套账号
      * @param bcrq 日期
      * @param zqdm 证券代码
      * @return
      */
    def zqlx(tzh: String, bcrq: String, zqdm: String): Boolean = {
      val condition1 = csbValues.value.get(tzh + CON01_KEY)
      if (condition1.isDefined && YES.equals(condition1.get)) {
        val condition2 = csbValues.value.get(tzh + "债券类型取债券品种信息维护的债券类型启用日期")
        if (condition2.isDefined && bcrq.compareTo(condition2.get) >= 0) {
          val condition3 = csbValues.value.get(tzh + "债券类型取债券品种信息维护的债券类型代码段")
          if (condition3.isDefined) {
            val array = condition3.get.split(SEPARATE2)
            for (ele <- array) {
              if (zqdm.startsWith(ele)) return true
            }
          }
        }
      }
      return false
    }

    /**
      * 获取套账号
      *
      * @param gddm 股东代码
      * @return
      */
    def getTzh(gddm: String): String = {
      csgdzhValues.value.getOrElse(gddm,DEFAULT_VALUE)
    }

    /**
      * 规则一、获取证券标志
      *
      * @param zqdm 证券代码
      * @param cjjg 成交金额
      * @return
      */
    def getZqbz(zqdm: String, cjjg: String, bcrq: String): String = {
      if (zqdm.startsWith("6")) {
        if (zqdm.startsWith("609")) return "CDRGP"
        return "GP"
      }
      if (zqdm.startsWith("5")) {
        if (cjjg.equals("0")) {
          if (jjxxbwh("ETF", zqdm, bcrq, 1)) return "EJDM"
          if (jjxxbwh("ETF", zqdm, bcrq, 2)) return "XJTD"
          if (jjxxbwh("ETF", zqdm, bcrq, 5)) return "XJTD_KSC"
        }
        return "JJ"
      }
      if (zqdm.startsWith("0") || zqdm.startsWith("1")) return "ZQ"
      if (zqdm.startsWith("20")) return "HG"
      if (zqdm.startsWith("58")) return "QZ"
      if (zqdm.startsWith("712") || zqdm.startsWith("730") || zqdm.startsWith("731")
        || zqdm.startsWith("732") || zqdm.startsWith("780") || zqdm.startsWith("734")
        || zqdm.startsWith("740") || zqdm.startsWith("790")) return "XG"
      if (zqdm.startsWith("733") || zqdm.startsWith("743") || zqdm.startsWith("751")
        || zqdm.startsWith("753") || zqdm.startsWith("762") || zqdm.startsWith("764")
        || zqdm.startsWith("793") || zqdm.startsWith("783")) return "XZ"
      if (zqdm.startsWith("742")) return "QY"
      if (zqdm.startsWith("714") || zqdm.startsWith("760") || zqdm.startsWith("781")) {
        if (sfspggp(zqdm, bcrq)) return "QY"
        return "XG"
      }
      if (zqdm.startsWith("73")) return "XG"
      if (zqdm.startsWith("70")) {
        if ("100".equals(cjjg)) return "XZ"
        val str = sfspggp(zqdm, bcrq)
        if (sfspggp(zqdm, bcrq)) return "QY"
        return "XG"
      }
      //如果没有匹配到就返回空
      DEFAULT_VALUE
    }

    /**
      * 规则二、获取业务标识
      *
      * @param zqbz 证券标识
      * @param zqdm 证券代码
      * @param cjjg 成交金额
      * @param bs   买卖
      * @return
      */
    def getYwbz(tzh: String, zqbz: String, zqdm: String, cjjg: String, bs: String, gsdm: String, bcrq: String): String = {
      if ("GP".equals(zqbz) || "CDRGP".equals(zqbz)) {
        val condition = csbValues.value.get(tzh + CON02_KEY)
        if (condition.isDefined && YES.equals(condition.get)) {
          //gh文件中的gsdm字段在CsQsXw表中有数据
          val maybeString = csqsxwValue.value.get(tzh + SEPARATE1 + gsdm)
          if (maybeString.isDefined) {
            if (bcrq.compareTo(maybeString.get) >= 0) return "ZS"
          }
          //zqdm字段在CsTsKm表中有数据
          val maybeString1 = csTsKmValue.value.get(1 + SEPARATE1 + 3 + SEPARATE1 + zqdm)
          if (maybeString1.isDefined) {
            if (bcrq.compareTo(maybeString1.get) >= 0) return "ZS"
          }
          //zqdm字段在CsTsKm表中有数据
          val maybeString2 = csTsKmValue.value.get(1 + SEPARATE1 + 2 + SEPARATE1 + zqdm)
          if (maybeString2.isDefined) {
            if (bcrq.compareTo(maybeString2.get) >= 0) return "ZB"
          }
        }

        //LSetCsSysJj表中FJJLX=0 (WHERE FSETCODE=套帐号)
        val lset = lsetcssysjjValues.value.getOrElse(tzh, DEFAULT_VALUE4).split(SEPARATE1)
        if ("0".equals(lset(0))) {
          if ("1".equals(lset(1)) || "5".equals(lset(1)) || "7".equals(lset(1))) {
            //gh文件中的gsdm字段在CsQsXw表中有数据
            val maybeString = csqsxwValue.value.get(tzh + SEPARATE1 + gsdm)
            if (maybeString.isDefined) {
              if (bcrq.compareTo(maybeString.get) >= 0) return "ZS"
            }
            //zqdm字段在CsTsKm表中有数据
            val maybeString1 = csTsKmValue.value.get(1 + SEPARATE1 + 3 + SEPARATE1 + zqdm)
            if (maybeString1.isDefined) {
              if (bcrq.compareTo(maybeString1.get) >= 0) return "ZS"
            }
          }
        }
        if ("0".equals(lset(0)) && "2".equals(lset(1))) {
          //zqdm字段在CsTsKm表中有数据
          val maybeString2 = csTsKmValue.value.get(1 + SEPARATE1 + 2 + SEPARATE1 + zqdm)
          if (maybeString2.isDefined) {
            if (bcrq.compareTo(maybeString2.get) >= 0) return "ZB"
          }
        }
        return "PT"
      }
      if ("EJDM".equals(zqbz) || "XJTD".equals(zqbz) || "XJTD_KSC".equals(zqbz)) {
        if ("B".equals(bs)) return "ETFSG"
        else return "ETFSH"
      }
      if ("JJ".equals(zqbz)) {
        if (zqdm.startsWith("501") || zqdm.startsWith("502")) return "LOF"
        if (zqdm.startsWith("518")) return "HJETF"
        if ("0".equals(cjjg) && (jjxxbwh("ETF", zqdm, bcrq, 0) || jjxxbwh("ETF", zqdm, bcrq, 1))) {
          if ("B".equals(bs)) return "ETFSG"
          else return "ETFSH"
        }
        if (jjxxbwh("ETF", zqdm, bcrq, 2) || jjxxbwh("ETF", zqdm, bcrq, 5)) {
          if ("B".equals(bs)) return "ETFSG"
          else return "ETFSH"
        }
        if (jjxxbwh("ETF", zqdm, bcrq, 3)) {
          if ("B".equals(bs)) return "ETFRG"
          else return "ETFFK"
        }
        if (jjxxbwh("ETF", zqdm, bcrq, 4)) {
          if ("B".equals(bs)) return "ETFRGZQ"
        }
        if (jjxxbwh("HB", zqdm, bcrq, 6)) {
          return "HBETF"
        }
        if (jjxxbwh("ETF", zqdm, bcrq, 0)) {
          return "ETF"
        }
        return "FBS"
      }
      if ("ZQ".equals(zqbz)) {
        if (zqlx(tzh, bcrq, zqdm)) {
          val condition = cszqxxValues._1.value.get(zqdm)
          if (condition.isDefined) {
            val lx = condition.get
            if ("国债".equals(lx)) return "GZXQ"
            if ("地方债券".equals(lx)) return "DFZQ"
            if ("可转换债券".equals(lx)) return "KZZ"
            if ("可交换公司债券".equals(lx)) return "KJHGSZQ"
            if ("资产证券".equals(lx)) return "ZCZQ"
            if ("次级债券".equals(lx)) return "CJZQ"
            if ("企业债券".equals(lx)) return "QYZQ"
            if ("私募债券".equals(lx)) return "SMZQ"
            if ("分离可转债".equals(lx)) return "FLKZZ"
            if ("可交换私募债券".equals(lx)) return "KJHSMZQ"
            if ("金融债券".equals(lx)) return "JRZQ"
            if ("金融债券_政策性".equals(lx)) return "JRZQ_ZCX"
          }
        }
        if (zqdm.startsWith("0")) {
          if (zqdm.startsWith("018")) return "JRZQ_ZCX"
          else return "GZXQ"
        }
        if (zqdm.startsWith("1")) {
          if (zqdm.startsWith("10") || zqdm.startsWith("11")) return "KZZ"
          if (zqdm.startsWith("121") || zqdm.startsWith("131")) return "ZCZQ"
          // zqdm符合正则表达式：123[0-4][0-9]{2}
          val regex = "123[0-4][0-9]{2}".r
          if (regex.findFirstMatchIn(zqdm).isDefined) return "CJZQ"
          if (zqdm.startsWith("137")) return "KJHSMZQ"
          if (zqdm.startsWith("130") || zqdm.startsWith("140") || zqdm.startsWith("147")) return "DFZQ"
          if (zqdm.startsWith("132") && "0".equals(cjjg)) return "KJHGSZQ"
          if ("0".equals(cjjg)) return "KZZGP"
          if (zqdm.startsWith("132")) return "KJHGSZQ"
          var res = cszqxxValues._2.value.get(zqdm + SEPARATE1 + tzh)
          if (res.isDefined && bcrq.compareTo(res.get) >= 0) {
            return "FLKZZ"
          } else {
            res = cszqxxValues._2.value.get(zqdm + SEPARATE1 + " ")
            if (res.isDefined && bcrq.compareTo(res.get) >= 0) return "FLKZZ"
          }
          return "QYZQ"

        }
      }
      if ("HG".equals(zqbz)) {
        if ("B".equals(bs)) {
          if (zqdm.startsWith("201") || zqdm.startsWith("203")
            || zqdm.startsWith("204") || zqdm.startsWith("205")) return "MCHG"
          else if (zqdm.startsWith("22")) return "MCHG_QY"
          else return "MC"
        } else {
          if (zqdm.startsWith("201") || zqdm.startsWith("203")
            || zqdm.startsWith("204") || zqdm.startsWith("205")) return "MRHG"
          else if (zqdm.startsWith("22")) return "MRHG_QY"
          else return "MR"
        }
      }
      if ("QZ".equals(zqbz)) {
        val last = zqdm.substring(zqdm.length - 3)
        if (zqdm.startsWith("580")) {
          if (last >= "000" && last <= "999") return "RGQZ"
          else return "RZQZ"
        } else {
          if (last >= "000" && last <= "999") return "RGQZXQ"
          else return "RZQZXQ"
        }
      }
      if ("XG".equals(zqbz)) {
        if (zqdm.startsWith("734") || zqdm.startsWith("740") || zqdm.startsWith("790")) {
          if ("B".equals(bs)) return "XGSG"
          else return "XGFK"
        }
        if (zqdm.startsWith("730") || zqdm.startsWith("732") || zqdm.startsWith("780")) return "XGZQ"
        if (zqdm.startsWith("712") || zqdm.startsWith("731")) return "XGZF"
        if (zqdm.startsWith("783")) return "KZZZQ"
        if (zqdm.startsWith("753") || zqdm.startsWith("762") || zqdm.startsWith("764")) return "KZZXZ"
        if (zqdm.startsWith("714") || zqdm.startsWith("760") || zqdm.startsWith("781") || zqdm.startsWith("70")) {
          //LSetCsSysJj表中FJJLX=0 并且 FJJLB=1，5，7  并且 （gsdm是指数席位号 || zqdm 是维护的指数债券）
          val lset = lsetcssysjjValues.value.getOrElse(tzh, DEFAULT_VALUE4).split(SEPARATE1)
          if ("0".equals(lset(0))) {
            if ("1".equals(lset(1)) || "5".equals(lset(1)) || "7".equals(lset(1))) {
              //gh文件中的gsdm字段在CsQsXw表中有数据
              val maybeString = csqsxwValue.value.get(tzh + SEPARATE1 + gsdm)
              if (maybeString.isDefined) {
                if (bcrq.compareTo(maybeString.get) >= 0) return "ZSPSZFZQ"
              }
              //zqdm字段在CsTsKm表中有数据
              val maybeString1 = csTsKmValue.value.get(1 + SEPARATE1 + 3 + SEPARATE1 + zqdm)
              if (maybeString1.isDefined) {
                if (bcrq.compareTo(maybeString1.get) >= 0) return "ZSPSZFZQ"
              }
            }
          }
          return "PSZFZQ"
        }
        if (zqdm.startsWith("73")) return "SHZQ"
      }
      if ("XZ".equals(zqbz)) {
        if (zqdm.startsWith("743") || zqdm.startsWith("793")) {
          if ("B".equals(bs)) return "KZZSG"
          else return "KZZFK"
        }
        if (zqdm.startsWith("783") || zqdm.startsWith("733")) return "KZZZQ"
        if (zqdm.startsWith("753") || zqdm.startsWith("762") || zqdm.startsWith("764")) return "KZZXZ"
        if (zqdm.startsWith("751")) return "QYZQXZ"
        if (zqdm.startsWith("70") && "100".equals(cjjg)) return "KZZXZ"
      }
      if ("QY".equals(zqbz)) {
        if (zqdm.startsWith("714") || zqdm.startsWith("760") || zqdm.startsWith("781")
          || zqdm.startsWith("742") || zqdm.startsWith("70")) {
          //LSetCsSysJj表中FJJLX=0 并且 FJJLB=1，5，7  并且 （gsdm是指数席位号 || zqdm 是维护的指数债券）
          val lset = lsetcssysjjValues.value.getOrElse(tzh, DEFAULT_VALUE4).split(SEPARATE1)
          if ("0".equals(lset(0))) {
            if ("1".equals(lset(1)) || "5".equals(lset(1)) || "7".equals(lset(1))) {
              //gh文件中的gsdm字段在CsQsXw表中有数据
              val maybeString = csqsxwValue.value.get(tzh + SEPARATE1 + gsdm)
              if (maybeString.isDefined) {
                if (bcrq.compareTo(maybeString.get) >= 0) return "ZSPSZFZQ"
              }
              //zqdm字段在CsTsKm表中有数据
              val maybeString1 = csTsKmValue.value.get(1 + SEPARATE1 + 3 + SEPARATE1 + zqdm)
              if (maybeString1.isDefined) {
                if (bcrq.compareTo(maybeString1.get) >= 0) return "ZSPSZFZQ"
              }
            }
          }
          return "PG"
        }
      }
      DEFAULT_VALUE
    }

    /**
      * 规则3、转换证券代码
      *
      * @param zqdm 证券代码
      * @param cjjg 成交金额
      * @return
      */
    def getZqdm(zqdm: String, cjjg: String): String = {
      val other3 = zqdm.substring(3)
      val other1 = zqdm.substring(1)
      if (zqdm.startsWith("730") || zqdm.startsWith("731") || zqdm.startsWith("737")) return "600" + other3
      if (zqdm.startsWith("732") || zqdm.startsWith("742")) return "603" + other3
      if (zqdm.startsWith("760") || zqdm.startsWith("780") || zqdm.startsWith("781")) return "601" + other3
      if (zqdm.startsWith("712") || zqdm.startsWith("714")) return "609" + other3
      if (zqdm.startsWith("733") || zqdm.startsWith("743")) return "110" + other3
      if (zqdm.startsWith("753") || zqdm.startsWith("762") || zqdm.startsWith("764")
        || zqdm.startsWith("783") || zqdm.startsWith("793")) return "113" + other3
      if (zqdm.startsWith("751")) return "112" + other3
      if (zqdm.startsWith("739")) return "002" + other3
      if (zqdm.startsWith("70") && cjjg.equals("100")) return "110" + other3
      if (zqdm.startsWith("70")) return "6" + other1
      zqdm
    }

    /**
      * 规则4、转换成交数量cjsl
      *
      * @param zqdm 证券代码
      * @param cjjg 成交价格
      * @param cjsl 成交数量
      * @return
      */
    def getCjsl(zqdm: String, cjjg: String, cjsl: String): String = {
      if (zqdm.startsWith("0") || zqdm.startsWith("1") || zqdm.startsWith("2")
        || zqdm.startsWith("733") || zqdm.startsWith("743") || zqdm.startsWith("783")
        || zqdm.startsWith("751") || zqdm.startsWith("753") || zqdm.startsWith("762")
        || zqdm.startsWith("764") || (zqdm.startsWith("70") && cjjg.equals("100"))) {
        return BigDecimal(cjsl).*(BigDecimal(10)).setScale(DEFAULT_DIGIT, RoundingMode.HALF_UP).formatted(DEFAULT_DIGIT_FORMAT)
      }
      cjsl
    }

    /**
      * 规则5、转换成交金额
      *
      * @param tzh  套账号
      * @param zqdm 证券代码
      * @param zqbz 证券标志
      * @param ywbz 业务标志
      * @param cjje 成交价格
      * @param cjsl 成交数量
      * @param cjjg 成交金额
      * @param gzlx 国债利息
      * @return
      */
    def getCjje(tzh: String, zqdm: String, zqbz: String, ywbz: String, cjje: String, cjsl: String, cjjg: String, gzlx: String): String = {
      if ("JJ".equals(zqbz)) {
        if ("ETFRG".equals(ywbz) || "ETFFK".equals(ywbz) || "ETFRGZQ".equals(ywbz))
          return BigDecimal(cjsl).*(BigDecimal(cjjg)).setScale(DEFAULT_DIGIT, RoundingMode.HALF_UP).formatted(DEFAULT_DIGIT_FORMAT)
      }

      if ("3".equals(lsetcssysjjValues.value.getOrElse(tzh, DEFAULT_VALUE4).split(SEPARATE1)(0)) || "-1".equals(cjje)) {
        return BigDecimal(cjsl).*(BigDecimal(cjjg)).setScale(DEFAULT_DIGIT, RoundingMode.HALF_UP).formatted(DEFAULT_DIGIT_FORMAT)
      }
      if ("HG".equals(zqbz) && zqdm.startsWith("203")) {
        return BigDecimal(cjsl).*(BigDecimal(100)).setScale(DEFAULT_DIGIT, RoundingMode.HALF_UP).formatted(DEFAULT_DIGIT_FORMAT)
      }
      if ("ZQ".equals(zqbz)) {
        if ("KJHSMZQ".equals(ywbz) || "KJHGSZQ".equals(ywbz)) {
          return BigDecimal(cjje).+(BigDecimal(gzlx)).setScale(DEFAULT_DIGIT, RoundingMode.HALF_UP).formatted(DEFAULT_DIGIT_FORMAT)
        }
      }
      return cjje
    }

    /**
      * 勾选了“上交所是否启用企债净价交易”则获取工作日
      *
      * @param bcrq 源文件日期
      * @param tzh  套账号
      * @return
      */
    def getFindate(bcrq: String, tzh: String) = {
      val value = csbValues.value.get(tzh + CON03_KEY)
      if (value.isDefined && YES.equals(value.get)) csholiday
      else bcrq
    }

    /**
      * 进行日期的格式化，基础信息表日期是 yyyy-MM-dd,原始数据是 yyyyMMdd
      * 这里将原始数据转换成yyyy-MM-dd的格式
      * @param bcrq
      * @return yyyy-MM-dd
      */
    def convertDate(bcrq:String) = {
      val yyyy = bcrq.substring(0,4)
      val mm = bcrq.substring(4,6)
      val dd = bcrq.substring(6)
      yyyy.concat(SEPARATE3).concat(mm).concat(SEPARATE3).concat(dd)
    }

    // 向df原始数据中添加 zqbz和ywbz 转换证券代码，转换成交金额，成交数量，也即处理规则1，2，3，4，5
    val etlRdd = df.rdd.map(row => {
      val gddm = row.getAs[String](0)
      val gdxm = row.getAs[String](1)
      var bcrq = row.getAs[String](2)
      bcrq = convertDate(bcrq)
      val cjbh = row.getAs[String](3)
      var gsdm = row.getAs[String](4)
      var cjsl = row.getAs[String](5)
      val bcye = row.getAs[String](6)
      var zqdm = row.getAs[String](7)
      val sbsj = row.getAs[String](8)
      val cjsj = row.getAs[String](9)
      val cjjg = row.getAs[String](10)
      var cjje = row.getAs[String](11)
      val sqbh = row.getAs[String](12)
      val bs = row.getAs[String](13)
      val mjbh = row.getAs[String](14)
      val zqbz = getZqbz(zqdm, cjjg, bcrq)
      val tzh = getTzh(gddm)
      val ywbz = getYwbz(tzh, zqbz, zqdm, cjjg, bs, gsdm, bcrq)
      val gzlv = gzlx(zqbz, zqdm, bcrq, cjsl)
      val findate = getFindate(bcrq, tzh)
      zqdm = getZqdm(zqdm, cjjg)
      cjsl = getCjsl(zqdm, cjjg, cjsl)
      cjje = getCjje(tzh, zqdm, zqbz, ywbz, cjje, cjsl, cjjg, gzlv)
      //gsdm不够5位补0
      var length = gsdm.length
      while (length < 5) {
        gsdm = "0" + gsdm
        length += 1
      }
      ShghYssj(gddm, gdxm, bcrq, cjbh, gsdm, cjsl, bcye, zqdm, sbsj, cjsj, cjjg, cjje, sqbh, bs, mjbh, zqbz, ywbz, tzh, gzlv, findate)
    })
    etlRdd
  }

  /** 汇总然后进行计算 */
  def doSum(spark: SparkSession, df: DataFrame, csb: Broadcast[collection.Map[String, String]],tableName:String) = {

    val sc = spark.sparkContext

    /** 加载公共费率表和佣金表 */
    def loadFeeTables() = {
      //公共的费率表
      val flbPath = Util.getDailyInputFilePath(TATABLE_NAME_JYLV)
      val flb = sc.textFile(flbPath)
      //117的佣金利率表
      val yjPath = Util.getDailyInputFilePath(TABLE_NAME_A117CSJYLV)
      val yjb = sc.textFile(yjPath)

      /** * 读取基金信息表csjjxx */
      val csjjxxPath = Util.getDailyInputFilePath(TABLE_NAME_JJXX)
      val jjxxb = sc.textFile(csjjxxPath)

      //券商过户费承担方式
      val csqsfylvPath = Util.getDailyInputFilePath(TABLE_NAME_CSQSFYLV)
      val csqsfy = sc.textFile(csqsfylvPath)
      val csqsfyMap = csqsfy.map(row => {
        val fields = row.split(SEPARATE2)
        val tzh = fields(0)
        val fzqpz = fields(2)
        val sh = fields(3)
        val ffylb = fields(4)
        val ffyfs = fields(5)
        val flv = fields(8)
        val ffymin = fields(9)
        val flvzk = fields(10)
        val key = tzh+SEPARATE1+fzqpz+SEPARATE1+sh+SEPARATE1+ffylb+SEPARATE1+ffyfs
        val value = flv+SEPARATE1+ffymin+SEPARATE1+flvzk
        (key,value)
      }).collectAsMap()

      //将佣金表转换成map结构
      val yjbMap = yjb.map(row => {
        val fields = row.split(SEPARATE2)
        val zqlb = fields(1) //证券类别
        val sh = fields(2) //市场
        val lv = fields(3) //利率
        val minLv = fields(4) //最低利率
        val startDate = fields(14) //启用日期
        //      val zch = row.get(15).toString // 资产
        val zk = fields(10) //折扣
        val fstr1 = fields(6) //交易席位/公司代码
        val key = zqlb + SEPARATE1 + sh + SEPARATE1 + fstr1 //证券类别+市场+交易席位/公司代码
        val value = startDate + SEPARATE1 + lv + SEPARATE1 + zk + SEPARATE1 + minLv //启用日期+利率+折扣+最低佣金值
        (key, value)
      })
        .groupByKey()
        .collectAsMap()

      //将费率表转换成map结构
      val flbMap = flb.map(row => {
        val fields = row.split(SEPARATE2)
        val zqlb = fields(0)
        //证券类别
        val sh = fields(1)
        //市场
        val lvlb = fields(2)
        //利率类别
        val lv = fields(3) //利率
        val zk = fields(5) //折扣
        val zch = fields(10) //资产号
        val startDate = fields(13)
        val fother = fields(6)
        //启用日期
        val key = zqlb + SEPARATE1 + sh + SEPARATE1 + zch + SEPARATE1 + lvlb //证券类别+市场+资产号+利率类别
        val value = startDate + SEPARATE1 + lv + SEPARATE1 + zk +SEPARATE1+fother//启用日期+利率+折扣+天数
        (key, value)
      })
        .groupByKey()
        .collectAsMap()

      //过滤基金信息表
      val jjxxAarry = jjxxb
        .filter(row => {
            val fields = row.split(SEPARATE2)
            val fzqlx = fields(9)
            val ftzdx = fields(15)
            val fszsh = fields(8)
          if("ETF".equals(fzqlx) && SH.equals(fszsh) && "ZQ".equals(ftzdx))  true
          else  false
          })
        .map(row => {
          val fields = row.split(SEPARATE2)
         fields(1)
      })
        .collect()

      (sc.broadcast(yjbMap), sc.broadcast(flbMap),sc.broadcast(jjxxAarry),sc.broadcast(csqsfyMap))
    }

    val broadcaseFee = loadFeeTables()
    val yjbValues = broadcaseFee._1
    val flbValues = broadcaseFee._2
    val jjxxValues = broadcaseFee._3
    val csqsfyValues = broadcaseFee._4
    val csbValues = csb

    /**
      * 原始数据转换1
      * key = 本次日期+证券代码+公司代码/交易席位+买卖+股东代码+套账号+证券标志+业务标志+读入日期
      */
    val groupedRdd = df.rdd.map(row => {
      val bcrq = row.getAs[String]("BCRQ") //本次日期
      val zqdm = row.getAs[String]("ZQDM") //证券代码
      val gsdm = row.getAs[String]("GSDM") //公司代码/交易席位
      val gddm = row.getAs[String]("GDDM") //股东代码
      val bs = row.getAs[String]("BS") //买卖
      val sqbh = row.getAs[String]("SQBH") //申请编号
      val tzh = row.getAs[String]("TZH") //套账号
      val zqbz = row.getAs[String]("FZQBZ") //证券标志
      val ywbz = row.getAs[String]("FYWBZ") //业务标志
      val findate = row.getAs[String]("FINDATE") //读入日期
      val key = bcrq + SEPARATE1 + zqdm + SEPARATE1 + gsdm + SEPARATE1 + bs + SEPARATE1 +
        gddm + SEPARATE1 + tzh + SEPARATE1 + zqbz + SEPARATE1 + ywbz + SEPARATE1 + findate
      (key, row)
    }).groupByKey()

    /**
      * 获取公共费率和佣金费率
      *
      * @param gsdm  交易席位/公司代码
      * @param bcrq  处理日期
      * @param ywbz1  业务标识
      * @param zqbz1  证券标识
      * @param zyzch 专用资产号
      * @param gyzch 公用资产号
      * @return
      */
    def getRate( zqdm:String,gsdm: String, gddm: String, bcrq: String, ywbz1: String, zqbz1: String, zyzch: String, gyzch: String) = {
      //为了获取启动日期小于等于处理日期的参数
      val flbMap = flbValues.value.mapValues(items => {
        val arr = items.toArray.filter(str => (bcrq.compareTo(str.split(SEPARATE1)(0)) >= 0)).sortWith((str1, str2) => (str1.split(SEPARATE1)(0).compareTo(str2.split(SEPARATE1)(0)) > 0))
        if (arr.size != 0)  arr(0)
        else DEFORT_VALUE3
      })
      val yjMap = yjbValues.value.mapValues(items => {
        val arr = items.toArray.filter(str => (bcrq.compareTo(str.split(SEPARATE1)(0)) >= 0)).sortWith((str1, str2) => (str1.split(SEPARATE1)(0).compareTo(str2.split(SEPARATE1)(0)) > 0))
        if (arr.size != 0) arr(0)
        else DEFORT_VALUE3
      })

      var ywbz = ywbz1
      var zqbz = zqbz1
      var sh = SH
      var jsf = JSF
      /** ETF类的要做特殊处理 */
      if(jjxxValues.value.contains(zqdm)) {
          ywbz = ETF_ZQBZ_OR_YWZ
          zqbz = ETF_ZQBZ_OR_YWZ
      }

      if(zqbz.startsWith(HG)){
        zqbz = HG + zqdm
        ywbz = HG + zqdm
        jsf = "SXF"
      }

      /**
        * 获取公共的费率
        * key = 证券类别+市场+资产号+利率类别
        * value = 启用日期+利率+折扣
        * 获取费率时默认的资产为117如果没有则资产改为0，还没有则费率就取0
        */
      def getCommonFee(fllb: String,zqbz:String,ywbz:String,sh:String,zyzch:String,gyzch:String) = {
        var rateStr = DEFORT_VALUE3
        var maybeRateStr = flbMap.get(ywbz + SEPARATE1 + sh + SEPARATE1 + zyzch + SEPARATE1 + fllb)
        if (maybeRateStr.isEmpty) {
          maybeRateStr = flbMap.get(zqbz + SEPARATE1 + sh + SEPARATE1 + zyzch + SEPARATE1 + fllb)
          if (maybeRateStr.isEmpty) {
            maybeRateStr = flbMap.get(ywbz + SEPARATE1 + sh + SEPARATE1 + gyzch + SEPARATE1 + fllb)
            if (maybeRateStr.isEmpty) {
              maybeRateStr = flbMap.get(zqbz + SEPARATE1 + sh + SEPARATE1 + gyzch + SEPARATE1 + fllb)
            }
          }
        }
        if (maybeRateStr.isDefined) {
          rateStr = maybeRateStr.get
        }

        val rate = rateStr.split(SEPARATE1)(1)
        val rateZk = rateStr.split(SEPARATE1)(2)
        val hgts = rateStr.split(SEPARATE1)(3)
        (rate, rateZk,hgts)
      }

      /**
        * 获取佣金费率
        * key=业务标志/证券标志+市场+交易席位/股东代码
        * value=启用日期+利率+折扣+最低佣金值
        */
      def getYjFee() = {
        var rateYJStr = DEFORT_VALUE3
        var maybeRateYJStr = yjMap.get(ywbz + SEPARATE1 + SH + SEPARATE1 + gsdm)
        if (maybeRateYJStr.isEmpty) {
           maybeRateYJStr = yjMap.get(ywbz + SEPARATE1 + SH + SEPARATE1 + gddm)
          if (maybeRateYJStr.isEmpty) {
             maybeRateYJStr = yjMap.get(zqbz + SEPARATE1 + SH + SEPARATE1 + gsdm)
            if (maybeRateYJStr.isEmpty) {
               maybeRateYJStr = yjMap.get(zqbz + SEPARATE1 + SH + SEPARATE1 + gddm)
            }
          }
        }

        if (maybeRateYJStr.isDefined) rateYJStr = maybeRateYJStr.get
        val rateYJ = rateYJStr.split(SEPARATE1)(1)
        val rateYjzk = rateYJStr.split(SEPARATE1)(2)
        val minYj = rateYJStr.split(SEPARATE1)(3)

        (rateYJ, rateYjzk, minYj)
      }

      /** 计算券商过户费 */
      def getQsFee() = {
        var qsghf = csqsfyValues.value.get(zyzch+SEPARATE1+zqbz+SEPARATE1+SH+SEPARATE1+QSGHF+SEPARATE1+FFYFS)
        if(qsghf.isEmpty){
          qsghf = csqsfyValues.value.get( gyzch+SEPARATE1+zqbz+SEPARATE1+SH+SEPARATE1+QSGHF+SEPARATE1+FFYFS)
          if(qsghf.isEmpty){
            qsghf = csqsfyValues.value.get(zyzch+SEPARATE1+ywbz+SEPARATE1+SH+SEPARATE1+QSGHF+SEPARATE1+FFYFS)
            if(qsghf.isEmpty){
              qsghf = csqsfyValues.value.get(gyzch+SEPARATE1+ywbz+SEPARATE1+SH+SEPARATE1+QSGHF+SEPARATE1+FFYFS)
            }
          }
        }
        var rateStr = DEFORT_VALUE2
        if(qsghf.isDefined) rateStr = qsghf.get
        val rate = rateStr.split(SEPARATE1)(0)
        val minRate = rateStr.split(SEPARATE1)(1)
        val rateZk = rateStr.split(SEPARATE1)(2)
        (rate,rateZk,minRate)
      }

      val rateJS = getCommonFee(jsf,zqbz,ywbz,sh,zyzch,gyzch)

      var rateYH = getCommonFee(YHS,zqbz,ywbz,sh,zyzch,gyzch)

      var rateZG = getCommonFee(ZGF,zqbz,ywbz,sh,zyzch,gyzch)

      var rateGH = getCommonFee(GHF,zqbz,ywbz,sh,zyzch,gyzch)

      var rateFXJ = getCommonFee(FXJ,zqbz,ywbz,sh,zyzch,gyzch)

      val yjFee = getYjFee()
      val qsghf = getQsFee()

      (rateJS._1, rateJS._2, rateYH._1, rateYH._2, rateZG._1, rateZG._2, rateGH._1, rateGH._2, rateFXJ._1, rateFXJ._2,
        yjFee._1, yjFee._2, yjFee._3,qsghf._1,qsghf._2,qsghf._3,rateJS._3)
    }

    /**
      * 根据套账号获取公共参数
      *
      * @param tzh 套账号
      **/
    def getGgcs(tzh: String) = {
      //获取是否的参数
      val cs1 = csbValues.value.getOrElse(tzh + CS1_KEY,NO) //是否开启佣金包含经手费，证管费
      var cs2 = csbValues.value.getOrElse(CS2_KEY, NO) //是否开启上交所A股过户费按成交金额计算
      val cs3 = csbValues.value.getOrElse(tzh + CS3_KEY,NO) //是否按千分之一费率计算过户费
      val cs4 = csbValues.value.getOrElse(tzh + CS4_KEY,NO) //是否开启计算佣金减去风险金
      val cs5 = csbValues.value.getOrElse(tzh + CS6_KEY,NO) //是否开启计算佣金减去结算费
      val cs6 = csbValues.value.getOrElse( CS7_KEY,DEFORT_ROUND).toInt //公共费率保留的小数位
      val cs7 = csbValues.value.getOrElse( CS8_KEY,DEFORT_ROUND).toInt //佣金保留的小数位
      //TODO 计算佣金的费率承担模式
      (cs1, cs2, cs3, cs4, cs5,cs6,cs7)
    }

    /**
      * 获取回购天数，计算回购收益
      *
      * @param cjjg 成交价格
      * @param cjsl 成交数量
      * @param zqdm 证券代码
      * @return 回购收益
      */
    def getHgsy( cjjg: BigDecimal, cjsl: BigDecimal, zqdm: String,hgts:String) = {
      val digit = csbValues.value.getOrElse(CON04_KEY,"5").toInt
      val ts = if(zqdm.startsWith("205")) BigDecimal(36500) else BigDecimal(36000)
       cjjg.*(BigDecimal(hgts))./(ts).setScale(digit,RoundingMode.HALF_UP).*(cjsl).*(BigDecimal(1000))
        .setScale(DEFAULT_DIGIT, RoundingMode.HALF_UP).formatted(DEFAULT_DIGIT_FORMAT)
    }

    /**
      * 根据套账号获取计算参数
      *
      * @param tzh 套账号
      * @return
      */
    def getJsgz(tzh: String) = {
      csbValues.value.getOrElse(tzh + CON8_KEY,NO) //是否开启实际收付金额包含佣金
    }

    //第一种  每一笔交易单独计算，最后相加
    val fee = groupedRdd.map  {
      case (key, values) =>
        val fields = key.split(SEPARATE1)
        val bs = fields(3) //买卖方向
        val gsdm = fields(2) //交易席位
        val bcrq = fields(0) //本次日期
        val zqdm = fields(1) //证券代码
        val gddm = fields(4)
        val tzh = fields(5)
        val zqbz = fields(6)
        val ywbz = fields(7)

        val getRateResult = getRate(zqdm,gsdm, gddm, bcrq, ywbz, zqbz, tzh, GYZCH)
        val rateJS: String = getRateResult._1
        val rateJszk: String = getRateResult._2
        val rateYH: String = getRateResult._3
        val rateYhzk: String = getRateResult._4
        val rateZG: String = getRateResult._5
        val rateZgzk: String = getRateResult._6
        val rateGH: String = getRateResult._7
        val rateGhzk: String = getRateResult._8
        val rateFXJ: String = getRateResult._9
        val rateFxjzk: String = getRateResult._10
        val rateYJ: String = getRateResult._11
        val rateYjzk: String = getRateResult._12
        val minYj: String = getRateResult._13
        val hgts:String = getRateResult._17

        val rateQsghf:String = getRateResult._14
        val rateQSghfZk:String = getRateResult._15
        val minQsghf = getRateResult._16
        val otherFee = BigDecimal(0)
        var sumCjje = BigDecimal(0) //总金额
        var sumCjsl = BigDecimal(0) //总数量
        var sumYj = BigDecimal(0) //总的佣金
        var sumJsf = BigDecimal(0) //总的经手费
        var sumYhs = BigDecimal(0) //总的印花税
        var sumZgf = BigDecimal(0) //总的征管费
        var sumGhf = BigDecimal(0) //总的过户费
        var sumFxj = BigDecimal(0) //总的风险金
        var sumGzlx = BigDecimal(0) //总的国债利息
        var sumHgsy = BigDecimal(0) //总的回购收益
        var sumQsghf = BigDecimal(0) //总的券商过户费

        val csResults = getGgcs(tzh)
        val cs1 = csResults._1
        var cs2 = csResults._2
        val cs3 = csResults._3
        val cs4 = csResults._4
        val cs5 = csResults._5
        val cs6 = csResults._6
        val cs7 = csResults._7

        for (row <- values) {
          val cjje = BigDecimal(row.getAs[String]("CJJE"))
          val cjjg = BigDecimal(row.getAs[String]("CJJG"))
          val cjsl = BigDecimal(row.getAs[String]("CJSL"))
          val gzlx = BigDecimal(row.getAs[String]("FGZLX"))
//          val hgsy = BigDecimal(row.getAs[String]("FHGGAIN"))
          // 经手费的计算
          val jsf = cjje.*(BigDecimal(rateJS)).*(BigDecimal(rateJszk)).setScale(cs6, RoundingMode.HALF_UP)
          var yhs = BigDecimal(0)
          // 买不计算印花税
          if (SALE.equals(bs)) {
            //印花税的计算
            yhs = cjje.*(BigDecimal(rateYH)).*(BigDecimal(rateYhzk)).setScale(cs6, RoundingMode.HALF_UP)
          }
          //征管费的计算
          val zgf = cjje.*(BigDecimal(rateZG)).*(BigDecimal(rateZgzk)).setScale(cs6, RoundingMode.HALF_UP)
          //风险金的计算
          val fx = cjje.*(BigDecimal(rateFXJ)).*(BigDecimal(rateFxjzk)).setScale(cs6, RoundingMode.HALF_UP)
          //过户费的计算
          var ghf = BigDecimal(0)

          var hgsy = getHgsy(cjjg,cjsl,zqdm,hgts)
          //券商过户费的计算
          var qsghf  = cjsl.*(BigDecimal(rateQsghf)).*(BigDecimal(rateQSghfZk)).setScale(cs6, RoundingMode.HALF_UP)
          if(qsghf < BigDecimal(minQsghf)) qsghf = BigDecimal(minQsghf)

          if (!(NO.equals(cs2) || YES.equals(cs2))) {
            //如果时日期格式的话要比较日期 TODO日期格式的比较
            if (bcrq.compareTo(cs2) > 0) {
              cs2 = YES
            } else {
              cs2 = NO
            }
          }
          if (YES.equals(cs2)) {
            if (YES.equals(cs3)) {
              ghf = cjjg.*(cjsl).*(BigDecimal(0.001)).setScale(cs6, RoundingMode.HALF_UP)
            } else {
              ghf = cjjg.*(cjsl).*(BigDecimal(rateGH)).*(BigDecimal(rateGhzk)).setScale(cs6, RoundingMode.HALF_UP)
            }
          } else {
            if (YES.equals(cs3)) {
              ghf = cjsl.*(BigDecimal(0.001)).setScale(cs6, RoundingMode.HALF_UP)
            } else {
              ghf = cjsl.*(BigDecimal(rateGH)).*(BigDecimal(rateGhzk)).setScale(cs6, RoundingMode.HALF_UP)
            }
          }
          //佣金的计算
          //          var yj = cjje.*(BigDecimal(rateYJ)).*(BigDecimal(rateYjzk)).setScale(DEFAULT_DIGIT, RoundingMode.HALF_UP)
          //          if (NO.equals(cs1)) {
          //            yj = yj.-(jsf).-(zgf)
          //          }
          //          if (YES.equals(cs4)) {
          //            yj = yj.-(fx)
          //          }
          //
          //          if (YES.equals(cs6)) {
          //            yj = yj.-(otherFee)
          //          }
          //          //单笔佣金小于最小佣金
          //          if (yj - BigDecimal(minYj) < 0) {
          //            yj = BigDecimal(minYj)
          //          }

          sumCjje = sumCjje.+(cjje)
          sumCjsl = sumCjsl.+(cjsl)
          //          sumYj = sumYj.+(yj)
          sumJsf = sumJsf.+(jsf)
          sumYhs = sumYhs.+(yhs)
          sumZgf = sumZgf.+(zgf)
          sumGhf = sumGhf.+(ghf)
          sumFxj = sumFxj.+(fx)
          sumGzlx = sumGzlx.+(gzlx)
          sumHgsy = sumHgsy.+(BigDecimal(hgsy))
          sumQsghf = sumQsghf.+(qsghf)
        }

        //佣金的计算
        sumYj = sumCjje.*(BigDecimal(rateYJ)).*(BigDecimal(rateYjzk)).setScale(cs7, RoundingMode.HALF_UP)
        if (NO.equals(cs1)) {
          sumYj = sumYj.-(sumJsf).-(sumZgf)
        }
        if (YES.equals(cs4)) {
          sumYj = sumYj.-(sumFxj)
        }

        if (YES.equals(cs5)) {
          sumYj = sumYj.-(otherFee)
        }
        sumYj = sumYj.-(sumQsghf)
        if (sumYj < BigDecimal(minYj)) {
          sumYj = BigDecimal(minYj)
        }

        (key, ShghFee(DEFAULT_VALUE, sumCjje, sumCjsl, sumYj, sumJsf, sumYhs, sumZgf,
          sumGhf, sumFxj, sumGzlx, sumHgsy))
    }

    //最终结果
    val result = fee.map {
      case (key, fee) =>
        val fields = key.split(SEPARATE1)
        val bs = fields(3) //买卖方向
        val gsdm = fields(2) //交易席位
        val bcrq = fields(0) //本次日期
        val zqdm = fields(1) //证券代码
        val gddm = fields(4)
        val tzh = fields(5)
        val zqbz = fields(6)
        val ywbz = fields(7)
        val findate = fields(8)

        val totalCjje = fee.sumCjje
        val totalCjsl = fee.sumCjsl
        val fgzlx = fee.sumGzlx
        val fhggain = fee.sumHgsy

        val realYj = fee.sumYj
        val realJsf = fee.sumJsf
        val realYhs = fee.sumYhs
        val realZgf = fee.sumZgf
        val realGhf = fee.sumGhf
        val realFxj = fee.sumFxj

        val con8 = getJsgz(tzh)
        var fsfje = BigDecimal(0)
        if(SALE.equals(bs)){
          fsfje = totalCjje.-(realJsf).-(realZgf).-(realGhf).-(realYhs)
        }else{
          fsfje = totalCjje.+(realJsf).+(realZgf).+(realGhf)
        }
        if (YES.equals(con8)) {
          if(SALE.equals(bs)){
            fsfje -= realYj
          }else{
            fsfje += realYj
          }
        }
        Hzjkqs(bcrq,
          findate, zqdm, SH, gsdm, bs,
          totalCjje.formatted(DEFAULT_DIGIT_FORMAT),
          totalCjsl.formatted(DEFAULT_DIGIT_FORMAT),
          realYj.formatted(DEFAULT_DIGIT_FORMAT),
          realJsf.formatted(DEFAULT_DIGIT_FORMAT),
          realYhs.formatted(DEFAULT_DIGIT_FORMAT),
          realZgf.formatted(DEFAULT_DIGIT_FORMAT),
          realGhf.formatted(DEFAULT_DIGIT_FORMAT),
          realFxj.formatted(DEFAULT_DIGIT_FORMAT),
          "0",
          fgzlx.formatted(DEFAULT_DIGIT_FORMAT),
          fhggain.formatted(DEFAULT_DIGIT_FORMAT),
          fsfje.formatted(DEFAULT_DIGIT_FORMAT),
          zqbz, ywbz,
          DEFAULT_VALUE, "N", zqdm, "PT", "1", DEFAULT_VALUE, DEFAULT_VALUE, "0", DEFAULT_VALUE, "0",
          gddm, DEFAULT_VALUE, DEFAULT_VALUE, DEFAULT_VALUE, DEFAULT_VALUE,
          DEFAULT_VALUE, DEFAULT_VALUE, "shgh", DEFAULT_VALUE, DEFAULT_VALUE, DEFAULT_VALUE, DEFAULT_VALUE, DEFAULT_VALUE
        )
    }
    //将结果输出
    import spark.implicits._
    Util.outputMySql(result.toDF(), tableName)
  }
}