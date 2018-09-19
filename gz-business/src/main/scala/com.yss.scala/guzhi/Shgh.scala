package com.yss.scala.guzhi

import com.yss.scala.dto.{ShghDto, ShghFee, ShghYssj}
import com.yss.scala.guzhi.ShghContants._
import com.yss.scala.util.Util
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.math.BigDecimal.RoundingMode

/**
  * @author ws
  * @version 2018-08-08
  *          描述：上海过户动态获取参数
  *          源文件：gdh.dbf
  *          结果表：SHDZGH
  */
object Shgh {

  def main(args: Array[String]): Unit = {
          testEtl()
  }

  /** 测试使用 */
  def testXX() = {
    val spark = SparkSession.builder().appName("SHDZGH").master("local[*]").getOrCreate()
    test01(spark)
    //TODO  ①把所有的基础表都广播出去 ②把一部分业务复杂的表广播出去其他的用join 3 都用join
    spark.stop()
  }

  def test01(spark: SparkSession) = {
    //读取基金信息表，并转换成map结构
    val csjjxxPath = "C:\\Users\\wuson\\Desktop\\GuZhi\\shuju\\CSJJXX"
    val csjjxx = spark.sparkContext.textFile(csjjxxPath)
      .filter(row => {
        val fields = row.split(SEPARATE2)
        val fsh = fields(10)
        val fszsh = fields(8)
        if ("1".equals(fsh) && "H".equals(fszsh)) true
        else false
      })
      .map(row => {
        val fields = row.split(SEPARATE2)
        val zqdm = fields(1) //证券代码
        val fzqlx = fields(9) //基金类型
        (zqdm + SEPARATE1 + fzqlx, row)
      })
      .groupByKey()
      .mapValues(rows => {
        //  fetftype='0'
        val filteredRows = rows.filter(str => str.split(",")(17).equals("0"))
        val ele1 =
          if (filteredRows.size == 0) ""
          else filteredRows.toArray.sortWith((str1, str2) => {
            str1.split(SEPARATE2)(14).compareTo(str2.split(SEPARATE2)(14)) < 0
          })(0)

        (ele1, rows.toArray.sortWith((str1, str2) => {
          str1.split(SEPARATE2)(14).compareTo(str2.split(SEPARATE2)(14)) < 0
        })(0))
      })

    val csjjxx01Map = csjjxx.map(row => {
      val fields = row._2._2.split(SEPARATE2)
      val FZQLX = fields(9)
      val FZQDMETF0 = fields(2)
      val FSTARTDATE = fields(14)
      (FZQLX + SEPARATE2 + FZQDMETF0, FSTARTDATE)
    }).collectAsMap()

    val csjjxx02Map = csjjxx.map(row => {
      val fields = row._2._2.split(SEPARATE2)
      val FZQLX = fields(9)
      val FZQDMETF0 = fields(4)
      val FSTARTDATE = fields(14)
      (FZQLX + SEPARATE2 + FZQDMETF0, FSTARTDATE)
    }).collectAsMap()
    val csjjxx05Map = csjjxx.map(row => {
      val fields = row._2._2.split(SEPARATE2)
      val FZQLX = fields(9)
      val FZQDMETF0 = fields(16)
      val FSTARTDATE = fields(14)
      (FZQLX + SEPARATE2 + FZQDMETF0, FSTARTDATE)
    }).collectAsMap()
    val csjjxx03Map = csjjxx.map(row => {
      val fields = row._2._2.split(SEPARATE2)
      val FZQLX = fields(9)
      val FZQDMETF0 = fields(5)
      val FSTARTDATE = fields(14)
      (FZQLX + SEPARATE2 + FZQDMETF0, FSTARTDATE)
    }).collectAsMap()
    val csjjxx04Map = csjjxx.map(row => {
      val fields = row._2._2.split(SEPARATE2)
      val FZQLX = fields(9)
      val FZQDMETF0 = fields(6)
      val FSTARTDATE = fields(14)
      (FZQLX + SEPARATE2 + FZQDMETF0, FSTARTDATE)
    }).collectAsMap()
    val csjjxx00Map = csjjxx.map(row => {
      val fields = row._2._2.split(SEPARATE2)
      val FZQLX = fields(9)
      val FZQDMETF0 = fields(2)
      val FSTARTDATE = fields(14)
      (FZQLX + SEPARATE2 + FZQDMETF0, FSTARTDATE)
    }).collectAsMap()
    val csjjxx01HPMap = csjjxx.filter(
      row => {
        !row._2._1.equals("")
      }
    ).map(row => {
      val fields = row._2._1.split(SEPARATE2)
      val FZQLX = fields(9)
      val FZQDMETF0 = fields(2)
      val FSTARTDATE = fields(14)
      (FZQLX + SEPARATE2 + FZQDMETF0, FSTARTDATE)
    }).collectAsMap()
    val csjjxxMap = Map((0, csjjxx00Map), (1, csjjxx01Map), (2, csjjxx02Map), (3, csjjxx03Map), (4, csjjxx04Map), (5, csjjxx05Map), (6, csjjxx01HPMap))
    csjjxx01HPMap.foreach(println(_))
  }

  def testEtl() = {
    val spark = SparkSession.builder().appName("SHGH").master("local[*]").getOrCreate()
    doETL(spark)
    spark.stop()
  }

  def loadTables(spark: SparkSession) = {

    val sc = spark.sparkContext

    def loadLvarlist() = {
      //公共的参数表
      val csbPath = Util.getDailyInputFilePath("LVARLIST")
      val csb = sc.textFile(csbPath)

      //将参数表转换成map结构
      val csbMap = csb.map(row => {
        val fields = row.split(SEPARATE2)
        val key = fields(0)
        //参数名
        val value = fields(1) //参数值
        (key, value)
      })
        .collectAsMap()
      sc.broadcast(csbMap)
    }

    /**
      * 读取基金信息表csjjxx，并广播
      */
    def loadCsjjxx() = {
      val csjjxxPath = Util.getDailyInputFilePath("CSJJXX")
      val csjjxx = sc.textFile(csjjxxPath)
        .filter(row => {
          val fields = row.split(SEPARATE2)
          val fsh = fields(10)
          val fszsh = fields(8)
          if ("1".equals(fsh) && "H".equals(fszsh)) true
          else false
        })
        .map(row => {
          val fields = row.split(SEPARATE2)
          val zqdm = fields(1) //证券代码
          val fzqlx = fields(9) //基金类型
          (zqdm + SEPARATE1 + fzqlx, row)
        })
        .groupByKey()
        .mapValues(rows => {
          //  fetftype='0'
          val filteredRows = rows.filter(str => str.split(",")(17).equals("0"))
          val ele1 =
            if (filteredRows.size == 0) ""
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
        val FZQDMETF0 = fields(2)
        val FSTARTDATE = fields(14)
        (FZQLX + SEPARATE2 + FZQDMETF0, FSTARTDATE)
      }).collectAsMap()
      val csjjxx02Map = csjjxx.map(row => {
        val fields = row._2._2.split(SEPARATE2)
        val FZQLX = fields(9)
        val FZQDMETF0 = fields(4)
        val FSTARTDATE = fields(14)
        (FZQLX + SEPARATE2 + FZQDMETF0, FSTARTDATE)
      }).collectAsMap()
      val csjjxx05Map = csjjxx.map(row => {
        val fields = row._2._2.split(SEPARATE2)
        val FZQLX = fields(9)
        val FZQDMETF0 = fields(16)
        val FSTARTDATE = fields(14)
        (FZQLX + SEPARATE2 + FZQDMETF0, FSTARTDATE)
      }).collectAsMap()
      val csjjxx03Map = csjjxx.map(row => {
        val fields = row._2._2.split(SEPARATE2)
        val FZQLX = fields(9)
        val FZQDMETF0 = fields(5)
        val FSTARTDATE = fields(14)
        (FZQLX + SEPARATE2 + FZQDMETF0, FSTARTDATE)
      }).collectAsMap()
      val csjjxx04Map = csjjxx.map(row => {
        val fields = row._2._2.split(SEPARATE2)
        val FZQLX = fields(9)
        val FZQDMETF0 = fields(6)
        val FSTARTDATE = fields(14)
        (FZQLX + SEPARATE2 + FZQDMETF0, FSTARTDATE)
      }).collectAsMap()
      val csjjxx00Map = csjjxx.map(row => {
        val fields = row._2._2.split(SEPARATE2)
        val FZQLX = fields(9)
        val FZQDMETF0 = fields(2)
        val FSTARTDATE = fields(14)
        (FZQLX + SEPARATE2 + FZQDMETF0, FSTARTDATE)
      }).collectAsMap()
      // 过滤 fetftype ='0' FZQLX='HB'的情况
      val csjjxx01HPMap = csjjxx.filter(
        row => {
          !row._2._1.equals("")
        }
      ).map(row => {
        val fields = row._2._1.split(SEPARATE2)
        val FZQLX = fields(9)
        val FZQDMETF0 = fields(2)
        val FSTARTDATE = fields(14)
        (FZQLX + SEPARATE2 + FZQDMETF0, FSTARTDATE)
      }).collectAsMap()
      val csjjxxMap = Map((0, csjjxx00Map), (1, csjjxx01Map), (2, csjjxx02Map), (3, csjjxx03Map), (4, csjjxx04Map), (5, csjjxx05Map), (6, csjjxx01HPMap))
      sc.broadcast(csjjxxMap)
    }

    /**
      * 加载权益信息表csqyxx，表并广播
      *
      * @return
      */
    def loadCsqyxx() = {
      val csqyxx = Util.readCSV("C:\\Users\\wuson\\Desktop\\GuZhi\\data\\csqyxx.csv", spark)
      val csqyxMap = csqyxx.rdd
        .map(row => {
          val fzqdm = row.getAs[String](2)
          (fzqdm, row)
        })
        .groupByKey()
        .collectAsMap()
      sc.broadcast(csqyxMap)
    }

    def loadCsqsxw() = {
      //读取CsOsXw表
      val csqsxwPath = Util.getDailyInputFilePath("CSQSXW")
      val csqsxw = sc.textFile(csqsxwPath)
        .filter(row => {
          val fields = row.split(SEPARATE2)
          val fxwlb = fields(4)
          val fsh = fields(6)
          if ("1".equals(fsh) && ("ZS".equals(fxwlb) || "ZYZS".equals(fxwlb))) true
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

    def loadCsTsKm() = {
      //读取CsTsKm表
      val csTsKmPath = Util.getDailyInputFilePath("A117CSTSKM")
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

    def loadCssysjj() = {
      /** 获取基金类型 */
      val lsetcssysjj = Util.readCSV("C:\\Users\\wuson\\Desktop\\GuZhi\\data\\LSETCSSYSJJ_201809051145.csv", spark)
      val lsetcssysjjMap = lsetcssysjj.rdd
        .map(row =>
          (row.getAs[String](0), row.getAs[String](1) + SEPARATE1 + row.getAs[String](3))
        )
        .collectAsMap()
      sc.broadcast(lsetcssysjjMap)
    }

    def loadCsqzxx() = {
      //读取CsZqXx表
      val csqzxxPath = Util.getDailyInputFilePath("A117CSTSKM")
      val csqzxxMap = sc.textFile(csqzxxPath)
        .map(row => {
          val fields = row.split(SEPARATE2)
          val zqdm = fields(0)
          val fzqlx = fields(11)
          (zqdm, fzqlx)
        }).collectAsMap()
      sc.broadcast(csqzxxMap)
    }

    def loadCsgdzh() = {
      //读取股东账号表，
      val csgdzhPath = Util.getDailyInputFilePath("CSGDZH")
      val csgdzhMap = sc.textFile(csgdzhPath)
        .map(row => {
          val fields = row.split(SEPARATE2)
          (fields(0), fields(5))
        }).collectAsMap()

      sc.broadcast(csgdzhMap)
    }

    (loadLvarlist(),loadCsjjxx(),loadCsqyxx(),loadCsqsxw(),loadCsTsKm(),loadCssysjj(),loadCsqzxx(),loadCsgdzh() )
//    (larlistValue, csjjxxValue,csqyxxValue, csqsxwValue, csTsKmValue, lsetcssysjjValue, csqzxxValue, csgdzhValue)
  }

  def doETL(spark: SparkSession) = {
    val sc = spark.sparkContext

    import com.yss.scala.dbf.dbf._
    //    val sourcePath = Util.getInputFilePath("gh23341.dbf")
    //原始数据
    val sourcePath = "C:\\Users\\wuson\\Desktop\\GuZhi\\shuju\\gh.dbf"
    val df = spark.sqlContext.dbfFile(sourcePath)

    //    (larlistValue, csjjxxValue,csqyxxValue, csqsxwValue, csTsKmValue, lsetcssysjjValue, csqzxxValue, csgdzhValue)
    val broadcaseValues = loadTables(spark)
    val csbValues = broadcaseValues._1
    val csjjxxValues = broadcaseValues._2
    val csqyxValues = broadcaseValues._3
    val csqsxwValue = broadcaseValues._4
    val csTsKmValue = broadcaseValues._5
    val lsetcssysjjValues = broadcaseValues._6
    val csqzxxValues = broadcaseValues._7
    val csgdzhValues = broadcaseValues._8

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
      val maybeString = map.get(fzqlx + SEPARATE2 + zqdm)
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
          val fqylx = String.valueOf(row.get(1))
          val fqydjr = String.valueOf(row.get(4))
          val fjkjzr = String.valueOf(row.get(6))
          val fstartdate = String.valueOf(row.get(11))
          val fqybl = String.valueOf(row.get(2))
          val fsh = String.valueOf(row.get(7))
          if ("PG".equals(fqylx) && "1".equals(fsh)
            && bcrq.compareTo(fqydjr) >= 0
            && bcrq.compareTo(fjkjzr) <= 0
            && bcrq.compareTo(fstartdate) >= 0
            && !condition1.contains(fqybl)) return true
        }
      }
      return false
    }

    /**
      * 获取证券标志
      *
      * @param zqdm 证券代码
      * @param cjjg 成交金额
      * @return
      */
    def getZqbz(zqdm: String, cjjg: String, bcrq: String): String = {
      if (zqdm.startsWith("6")) {
        if (zqdm.startsWith("609")) return "CDRGP"
        else return "GP"
      }
      if (zqdm.startsWith("5")) {
        if (cjjg.equals("0")) {
          if (jjxxbwh("ETF", zqdm, bcrq, 1)) return "EJDM"
          if (jjxxbwh("ETF", zqdm, bcrq, 2)) return "XJTD"
          if (jjxxbwh("ETF", zqdm, bcrq, 5)) return "XJTD_KSC"
        } else return "JJ"
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
      throw new Exception("无法找到对应的证券标识：" + zqdm)
    }

    /**
      * 判断ZQ的业务类型
      *
      * @param tzh
      * @param bcrq
      * @param zqdm
      * @return
      */
    def zqlx(tzh: String, bcrq: String, zqdm: String): Boolean = {
      val condition1 = csbValues.value.get(tzh + "债券类型取债券品种信息维护的债券类型")
      if (condition1.isDefined && "1".equals(condition1.get)) {
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
      * @param gddm
      * @return
      */
    def getTzh(gddm: String): String = {
      val maybe = csgdzhValues.value.get(gddm)
      if(maybe.isEmpty){
        throw new Exception("未找到对应的套账号："+gddm)
      }
      maybe.get
    }

    /**
      * 获取业务标识
      *
      * @param zqbz 证券标识
      * @param zqdm 证券代码
      * @param cjjg 成交金额
      * @param bs   买卖
      * @return
      */
    def getYwbz(tzh: String, zqbz: String, zqdm: String, cjjg: String, bs: String, gsdm: String, bcrq: String): String = {
      if ("GP".equals(zqbz) || "CDRGP".equals(zqbz)) {

        val condition = csbValues.value.get(tzh + "指数、指标股票按特殊科目设置页面处理")
        if (condition.isDefined && "1".equals(condition.get)) {
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
        val lset = lsetcssysjjValues.value.getOrElse(tzh, "-1@-1").split("@")
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
        } else return "FBS"
      }
      if ("ZQ".equals(zqbz)) {

        if (zqlx(tzh, bcrq, zqdm)) {
          val condition = csqzxxValues.value.get(zqdm)
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
          if (zqbz.startsWith("130") || zqdm.startsWith("140") || zqdm.startsWith("147")) return "DFZQ"
          if (zqdm.startsWith("132") && "0".equals(cjjg)) return "KJHGSZQ"
          if ("0".equals(cjjg)) return "KZZGP"
          if (zqdm.startsWith("132")) return "KJHGSZQ"
          //TODO CSZQXX表中维护了类型为FLKZZ的债券代码
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
          val lset = lsetcssysjjValues.value.getOrElse(tzh, "-1@-1").split("@")
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
        }
        if (zqdm.startsWith("73")) return "SHZQ"
      }
      if ("XZ".equals(zqbz)) {
        if (zqdm.startsWith("743") || zqdm.startsWith("793")) {
          if ("B".equals(bs)) return "KZZSG"
          else return "KZZFK"
        }
        if (zqdm.startsWith("783")) return "KZZZQ"
        if (zqdm.startsWith("753") || zqdm.startsWith("762") || zqdm.startsWith("764")) return "KZZXZ"
        if (zqdm.startsWith("751")) return "QYZQXZ"
        if (zqdm.startsWith("70") && "100".equals(cjjg)) return "KZZXZ"
      }
      if ("QY".equals(zqbz)) {
        if (zqdm.startsWith("714") || zqdm.startsWith("760") || zqdm.startsWith("781")
          || zqdm.startsWith("742") || zqdm.startsWith("70")) {
          //LSetCsSysJj表中FJJLX=0 并且 FJJLB=1，5，7  并且 （gsdm是指数席位号 || zqdm 是维护的指数债券）
          val lset = lsetcssysjjValues.value.getOrElse(tzh, "-1@-1").split("@")
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
          else return "PG"
        }
      }
      throw new Exception("")
    }

    /**
      * 转换证券代码
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
      * 转换cjje
      *
      * @param zqdm
      * @param cjjg
      * @return
      */
    def getCjsl(zqdm: String, cjjg: String, cjsl: String): String = {
      if (zqdm.startsWith("0") || zqdm.startsWith("1") || zqdm.startsWith("2")
        || zqdm.startsWith("733") || zqdm.startsWith("743") || zqdm.startsWith("783")
        || zqdm.startsWith("751") || zqdm.startsWith("753") || zqdm.startsWith("762")
        || zqdm.startsWith("764") || (zqdm.startsWith("70") && cjjg.equals("100"))) {
        return BigDecimal(cjsl).*(BigDecimal(10)).setScale(2, RoundingMode.HALF_UP).formatted("%.2f")
      }
      cjsl
    }

    /**
      * 转换cjje
      **/
    def getCjje(tzh: String, zqdm: String, zqbz: String, ywbz: String, cjje: String, cjsl: String, cjjg: String, gzlx: String): String = {
      if ("JJ".equals(zqbz)) {
        if ("ETFRG".equals(ywbz) || "ETFFK".equals(ywbz) || "ETFRGZQ".equals(ywbz))
          return BigDecimal(cjsl).*(BigDecimal(cjjg)).setScale(2, RoundingMode.HALF_UP).formatted("%.2f")
      }

      if ("3".equals(lsetcssysjjValues.value.getOrElse(tzh, "-1@-1").split("@")(0)) || "-1".equals(cjje)) {
        return BigDecimal(cjsl).*(BigDecimal(cjjg)).setScale(2, RoundingMode.HALF_UP).formatted("%.2f")
      }
      if ("HG".equals(zqbz) && zqdm.startsWith("203")) {
        return BigDecimal(cjsl).*(BigDecimal(100)).setScale(2, RoundingMode.HALF_UP).formatted("%.2f")
      }
      if ("ZQ".equals(zqbz)) {
        if ("KJHSMZQ".equals(ywbz) || "KJHGSZQ".equals(ywbz)) {
          return BigDecimal(cjje).+(BigDecimal(gzlx)).setScale(2, RoundingMode.HALF_UP).formatted("%.2f")
        }
      }
      return cjje
    }

    //TODO 向df原始数据中添加 zqbz和ywbz 转换证券代码，转换成交金额，成交数量，也即处理规则1，2，3，4，5
   val etlRdd = df.rdd.map(row => {
      val gddm = row.getAs[String]("GDDM")
      //      val gdxw = row.getAs[String]("GDXM")
      val bcrq = row.getAs[String]("BCRQ")
      val cjbh = row.getAs[String]("CJBH")
      var gsdm = row.getAs[String]("GSDM")
      val cjsl = row.getAs[String]("CJSL")
      val bcye = row.getAs[String]("BCYE")
      var zqdm = row.getAs[String]("ZQDM")
      val sbsj = row.getAs[String]("SBSJ")
      val cjsj = row.getAs[String]("CJSJ")
      val cjjg = row.getAs[String]("CJJG")
      val cjje = row.getAs[String]("CJJE")
      val sqbh = row.getAs[String]("SQBH")
      val bs = row.getAs[String]("BS")
      val mjbh = row.getAs[String]("MJBH")
      val zqbz = getZqbz(zqdm, cjjg, bcrq)
      val tzh = getTzh(gddm)
      val ywbz = getYwbz(tzh, zqbz, zqdm, cjjg, bs, gsdm, bcrq)
      //gsdm不够5位补0
      var length = gsdm.length
      while (length < 5) {
        gsdm = "0" + gsdm
        length += 1
      }
      ShghYssj(gddm, "", bcrq, cjbh, gsdm, cjsj, bcye, zqdm, sbsj, cjsj, cjjg, cjje, sqbh, bs, mjbh, zqbz, ywbz,tzh)
    })
    import spark.implicits._
    Util.outputMySql(etlRdd.toDF(),"shgh_etl_ws")

  }

  def doExec(df: DataFrame,csbValues: Broadcast[Map[String, String]]) = {

    val spark = SparkSession.builder().appName("SHGH").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    //TODO --------------计算--------------

    def loadFeeTables() = {
      //公共的费率表
      val flbPath = Util.getDailyInputFilePath("CSJYLV")
      val flb = sc.textFile(flbPath)
      //117的佣金利率表
      val yjPath = Util.getDailyInputFilePath("A117CSYJLV")
      val yjb = sc.textFile(yjPath)

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
        .mapValues(item => { //分组完成后进行排序取最大的启用日期的数据
          item.toArray.sortWith((str1, str2) => {
            str1.split(SEPARATE1)(0).compareTo(str2.split(SEPARATE1)(0)) > 0
          })
        })
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
        //启用日期
        val key = zqlb + SEPARATE1 + sh + SEPARATE1 + zch + SEPARATE1 + lvlb //证券类别+市场+资产号+利率类别
        val value = startDate + SEPARATE1 + lv + SEPARATE1 + zk //启用日期+利率+折扣
        (key, value)
      })
        .groupByKey()
        .mapValues(item => { //分组完成后进行排序
          item.toArray.sortWith((str1, str2) => {
            str1.split(SEPARATE1)(0).compareTo(str2.split(SEPARATE1)(0)) > 0
          })
        })
        .collectAsMap()

      (sc.broadcast(yjbMap), sc.broadcast(flbMap))
    }

    val broadcaseFee = loadFeeTables()
    val yjbValues = broadcaseFee._1
    val flbValues = broadcaseFee._2
    /**
      * 原始数据转换1
      * key = 本次日期+证券代码+公司代码/交易席位+买卖+申请编号
      */
    val value = df.rdd.map(row => {
      val bcrq = row.getAs[String]("BCRQ") //本次日期
      val zqdm = row.getAs[String]("ZQDM") //证券代码
      val gsdm = row.getAs[String]("GSDM") //公司代码/交易席位
      val gddm = row.getAs[String]("GDDM") //股东代码
      val bs = row.getAs[String]("BS") //买卖
      val sqbh = row.getAs[String]("SQBH") //申请编号
      val key = bcrq + SEPARATE1 + zqdm + SEPARATE1 + gsdm + SEPARATE1 + bs + SEPARATE1 + sqbh
      (key, row)
    }).groupByKey()

    /**
      * 原始数据转换1
      * key = 本次日期+证券代码+公司代码/交易席位+买卖
      */
    val value1 = df.rdd.map(row => {
      val bcrq = row.getAs[String]("BCRQ") //本次日期
      val zqdm = row.getAs[String]("ZQDM") //证券代码
      val gsdm = row.getAs[String]("GSDM") //公司代码/交易席位
      val gddm = row.getAs[String]("GDDM") //股东代码
      val bs = row.getAs[String]("BS") //买卖
      val sqbh = row.getAs[String]("SQBH") //申请编号
      val key = bcrq + SEPARATE1 + zqdm + SEPARATE1 + gsdm + SEPARATE1 + bs
      (key, row)
    }).groupByKey()

    /**
      * 获取公共费率和佣金费率
      *
      * @param gsdm  交易席位/公司代码
      * @param bcrq  处理日期
      * @param ywbz  业务标识
      * @param zqbz  证券标识
      * @param zyzch 专用资产号
      * @param gyzch 公用资产号
      * @return
      */
    def getRate(gsdm: String, bcrq: String, ywbz: String, zqbz: String, zyzch: String, gyzch: String) = {
      //为了获取启动日期小于等于处理日期的参数
      val flbMap = flbValues.value.mapValues(items => items.partition(_.split(SEPARATE1)(0).compareTo(bcrq) <= 0)._1(0))
      val yjMap = yjbValues.value.mapValues(items => items.partition(_.split(SEPARATE1)(0).compareTo(bcrq) <= 0)._1(0))

      /**
        * 获取公共的费率
        * key = 证券类别+市场+资产号+利率类别
        * value = 启用日期+利率+折扣
        * 获取费率时默认的资产为117如果没有则资产改为0，还没有则费率就取0
        */
      var rateJSstr = flbMap.getOrElse(ywbz + SEPARATE1 + SH + SEPARATE1 + zyzch + SEPARATE1 + JSF, DEFORT_VALUE1)
      if (DEFORT_VALUE1.equals(rateJSstr)) rateJSstr = flbMap.getOrElse(ywbz + SEPARATE1 + SH + SEPARATE1 + gyzch + SEPARATE1 + JSF, DEFORT_VALUE2)
      val rateJS = rateJSstr.split(SEPARATE1)(1)
      val rateJszk = rateJSstr.split(SEPARATE1)(2)

      var rateYHStr = flbMap.getOrElse(ywbz + SEPARATE1 + SH + SEPARATE1 + zyzch + SEPARATE1 + YHS, DEFORT_VALUE1)
      if (DEFORT_VALUE1.equals(rateYHStr)) rateYHStr = flbMap.getOrElse(ywbz + SEPARATE1 + SH + SEPARATE1 + gyzch + SEPARATE1 + YHS, DEFORT_VALUE2)
      val rateYH = rateYHStr.split(SEPARATE1)(1)
      val rateYhzk = rateYHStr.split(SEPARATE1)(2)

      var rateZGStr = flbMap.getOrElse(ywbz + SEPARATE1 + SH + SEPARATE1 + zyzch + SEPARATE1 + ZGF, DEFORT_VALUE1)
      if (DEFORT_VALUE1.equals(rateZGStr)) rateZGStr = flbMap.getOrElse(ywbz + SEPARATE1 + SH + SEPARATE1 + gyzch + SEPARATE1 + ZGF, DEFORT_VALUE2)
      val rateZG = rateZGStr.split(SEPARATE1)(1)
      val rateZgzk = rateZGStr.split(SEPARATE1)(2)

      var rateGHStr = flbMap.getOrElse(ywbz + SEPARATE1 + SH + SEPARATE1 + zyzch + SEPARATE1 + GHF, DEFORT_VALUE1)
      if (DEFORT_VALUE1.equals(rateGHStr)) rateGHStr = flbMap.getOrElse(ywbz + SEPARATE1 + SH + SEPARATE1 + gyzch + SEPARATE1 + GHF, DEFORT_VALUE2)
      val rateGH = rateGHStr.split(SEPARATE1)(1)
      val rateGhzk = rateGHStr.split(SEPARATE1)(2)

      var rateFXJStr = flbMap.getOrElse(ywbz + SEPARATE1 + SH + SEPARATE1 + zyzch + SEPARATE1 + FXJ, DEFORT_VALUE1)
      if (DEFORT_VALUE1.equals(rateFXJStr)) rateFXJStr = flbMap.getOrElse(ywbz + SEPARATE1 + SH + SEPARATE1 + gyzch + SEPARATE1 + FXJ, DEFORT_VALUE2)
      val rateFXJ = rateFXJStr.split(SEPARATE1)(1)
      val rateFxjzk = rateFXJStr.split(SEPARATE1)(2)

      /**
        * 获取佣金费率
        * key=证券类别+市场+交易席位/公司代码
        * value=启用日期+利率+折扣+最低佣金值
        */
      // var rateYJStr = yjbValues.value.getOrElse(ywbz + SEPARATE1 + SH + SEPARATE1 + gddm, DEFORT_VALUE1)
      // if (DEFORT_VALUE1.eq(rateYJStr))
      val rateYJStr = yjMap.getOrElse(ywbz + SEPARATE1 + SH + SEPARATE1 + gsdm, DEFORT_VALUE3)
      val rateYJ = rateYJStr.split(SEPARATE1)(1)
      val rateYjzk = rateYJStr.split(SEPARATE1)(2)
      val minYj = rateYJStr.split(SEPARATE1)(3)
      (rateJS, rateJszk, rateYH, rateYhzk, rateZG, rateZgzk, rateGH, rateGhzk, rateFXJ, rateFxjzk, rateYJ, rateYjzk, minYj)
    }

    /**
      * 根据套账号获取公共参数
      *
      * @param tzh 套账号
      **/
    def getGgcs(tzh: String) = {
      //获取是否的参数
      val cs1 = csbValues.value(tzh + CS1_KEY) //是否开启佣金包含经手费，证管费
      var cs2 = csbValues.value.getOrElse(tzh + CS2_KEY, NO) //是否开启上交所A股过户费按成交金额计算
      val cs3 = csbValues.value(tzh + CS3_KEY) //是否按千分之一费率计算过户费
      val cs4 = csbValues.value(tzh + CS4_KEY) //是否开启计算佣金减去风险金
      val cs5 = csbValues.value(tzh + CS6_KEY) //是否开启计算佣金减去结算费
      (cs1, cs2, cs3, cs4, cs5)
    }

    /**
      * 根据套账号获取计算参数
      *
      * @param tzh 套账号
      * @return
      */
    def getJsgz(tzh: String) = {
      val cs6 = csbValues.value(tzh + CON8_KEY) //是否开启实际收付金额包含佣金

      //获取计算参数
      val con1 = csbValues.value(tzh + CON1_KEY) //是否勾选按申请编号汇总计算经手费
      val con2 = csbValues.value(tzh + CON2_KEY) //是否勾选按申请编号汇总计算征管费
      val con3 = csbValues.value(tzh + CON3_KEY) //是否勾选按申请编号汇总计算过户费
      val con4 = csbValues.value(tzh + CON4_KEY) //是否勾选按申请编号汇总计算印花税
      val con5 = csbValues.value(tzh + CON5_KEY) //是否勾选H按申请编号汇总计算佣金
      val con6 = csbValues.value(tzh + CON7_KEY) //是否勾选H按申请编号汇总计算风险金

      val con7 = csbValues.value(tzh + CON11_KEY) //是否开启按成交记录计算经手费
      val con8 = csbValues.value(tzh + CON12_KEY) //是否开启按成交记录计算征管费
      val con9 = csbValues.value(tzh + CON13_KEY) //是否开启按成交记录计算过户费
      val con10 = csbValues.value(tzh + CON14_KEY) //是否开启按成交记录计算印花税
      val con11 = csbValues.value(tzh + CON15_KEY) //是否开启H按成交记录计算佣金
      val con12 = csbValues.value(tzh + CON17_KEY) //是否开启H按成交记录计算风险金
      (cs6, con1, con2, con3, con4, con5, con6, con7, con8, con9, con10, con11, con12)
    }

    /**
      * 获取套账号和资产类型
      *
      * @param gddm
      */
    def getOtherInfo(gddm: String) = {

    }

    //第一种  每一笔交易单独计算，最后相加
    val fee1 = value1.map {
      case (key, values) =>
        val fields = key.split(SEPARATE1)
        val bs = fields(3) //买卖方向
      val gsdm = fields(2) //交易席位
      val bcrq = fields(0) //本次日期
      val zqdm = fields(1) //证券代码

        //        getZqbz()
        val getRateResult = getRate(gsdm, bcrq, ZCLB, ZCLB, ZYZCH, GYZCH)
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

        val otherFee = BigDecimal(0)
        var sumCjje = BigDecimal(0) //总金额
      var sumCjsl = BigDecimal(0) //总数量
      var sumYj = BigDecimal(0) //总的佣金
      var sumJsf = BigDecimal(0) //总的经手费
      var sumYhs = BigDecimal(0) //总的印花税
      var sumZgf = BigDecimal(0) //总的征管费
      var sumGhf = BigDecimal(0) //总的过户费
      var sumFxj = BigDecimal(0) //总的风险金

        val csResults = getGgcs("117")
        val cs1 = csResults._1
        var cs2 = csResults._2
        val cs3 = csResults._3
        val cs4 = csResults._4
        val cs5 = csResults._5

        for (row <- values) {
          val cjje = BigDecimal(row.getAs[String]("CJJE"))
          val cjsl = BigDecimal(row.getAs[String]("CJSL"))
          // 经手费的计算
          val jsf = cjje.*(BigDecimal(rateJS)).*(BigDecimal(rateJszk)).setScale(2, RoundingMode.HALF_UP)
          var yhs = BigDecimal(0)
          // 买不计算印花税
          if (SALE.equals(bs)) {
            //印花税的计算
            yhs = cjje.*(BigDecimal(rateYH)).*(BigDecimal(rateYhzk)).setScale(2, RoundingMode.HALF_UP)
          }
          //征管费的计算
          val zgf = cjje.*(BigDecimal(rateZG)).*(BigDecimal(rateZgzk)).setScale(2, RoundingMode.HALF_UP)
          //风险金的计算
          val fx = cjje.*(BigDecimal(rateFXJ)).*(BigDecimal(rateFxjzk)).setScale(2, RoundingMode.HALF_UP)
          //过户费的计算
          var ghf = BigDecimal(0)

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
              ghf = cjje.*(cjsl).*(BigDecimal(0.001)).setScale(2, RoundingMode.HALF_UP)
            } else {
              ghf = cjje.*(cjsl).*(BigDecimal(rateGH)).*(BigDecimal(rateGhzk)).setScale(2, RoundingMode.HALF_UP)
            }
          } else {
            if (YES.equals(cs3)) {
              ghf = cjsl.*(BigDecimal(0.001)).setScale(2, RoundingMode.HALF_UP).setScale(2, RoundingMode.HALF_UP)
            } else {
              ghf = cjsl.*(BigDecimal(rateGH)).*(BigDecimal(rateGhzk)).setScale(2, RoundingMode.HALF_UP)
            }
          }
          //佣金的计算
          //          var yj = cjje.*(BigDecimal(rateYJ)).*(BigDecimal(rateYjzk)).setScale(2, RoundingMode.HALF_UP)
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
        }

        //佣金的计算
        sumYj = sumCjje.*(BigDecimal(rateYJ)).*(BigDecimal(rateYjzk)).setScale(2, RoundingMode.HALF_UP)
        if (NO.equals(cs1)) {
          sumYj = sumYj.-(sumJsf).-(sumZgf)
        }
        if (YES.equals(cs4)) {
          sumYj = sumYj.-(sumFxj)
        }

        if (YES.equals(cs5)) {
          sumYj = sumYj.-(otherFee)
        }
        if (sumYj < BigDecimal(minYj)) {
          sumYj = BigDecimal(minYj)
        }

        (key, ShghFee("1", sumCjje, sumCjsl, sumYj, sumJsf, sumYhs, sumZgf,
          sumGhf, sumFxj))
    }

    //第二种 相同申请编号的金额汇总*费率，各申请编号汇总后的金额相加
    val fee2 = value.map {
      case (key, values) => {
        val fields = key.split(SEPARATE1)
        val bs = fields(3) //买卖方向
        val gsdm = fields(2) //交易席位
        val bcrq = fields(0) //本次日期
        val zqdm = fields(1) //证券代码
        val otherFee = BigDecimal(0)

        val getRateResult = getRate(gsdm, bcrq, ZCLB, ZCLB, ZYZCH, GYZCH)
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

        var sumCjje = BigDecimal(0) //同一个申请编号总金额
        var sumCjsl = BigDecimal(0) //同一个申请编号总数量

        val csResults = getGgcs("117")
        val cs1 = csResults._1
        var cs2 = csResults._2
        val cs3 = csResults._3
        val cs4 = csResults._4
        val cs5 = csResults._5

        for (row <- values) {
          val cjje = BigDecimal(row.getAs[String]("CJJE"))
          val cjsl = BigDecimal(row.getAs[String]("CJSL"))
          sumCjje = sumCjje.+(cjje)
          sumCjsl = sumCjsl.+(cjsl)
        }

        var sumJsf2 = sumCjje.*(BigDecimal(rateJS)).*(BigDecimal(rateJszk)).setScale(2, RoundingMode.HALF_UP)
        //同一个申请编号总的经手费
        var sumYhs2 = BigDecimal(0) //同一个申请编号总的印花税
        if (SALE.equals(bs)) {
          sumYhs2 = sumCjje.*(BigDecimal(rateYH)).*(BigDecimal(rateYhzk)).setScale(2, RoundingMode.HALF_UP)
        }
        var sumZgf2 = sumCjje.*(BigDecimal(rateZG)).*(BigDecimal(rateZgzk)).setScale(2, RoundingMode.HALF_UP) //同一个申请编号总的征管费
        var sumFxj2 = sumCjje.*(BigDecimal(rateFXJ)).*(BigDecimal(rateFxjzk)).setScale(2, RoundingMode.HALF_UP) //同一个申请编号总的风险金
        //同一个申请编号总的过户费
        var sumGhf2 = BigDecimal(0)

        if (!(NO.equals(cs2) || (YES).equals(cs2))) {
          //如果时日期格式的话要比较日期 TODO日期格式的比较
          if (bcrq.compareTo(cs2) > 0) {
            cs2 = YES
          } else {
            cs2 = NO
          }
        }
        if (YES.equals(cs2)) {
          if (YES.equals(cs3)) {
            sumGhf2 = sumCjje.*(sumCjsl).*(BigDecimal(0.001)).setScale(2, RoundingMode.HALF_UP)
          } else {
            sumGhf2 = sumCjje.*(sumCjsl).*(BigDecimal(rateGH)).*(BigDecimal(rateGhzk)).setScale(2, RoundingMode.HALF_UP)
          }
        } else {
          if (YES.equals(cs3)) {
            sumGhf2 = sumCjsl.*(BigDecimal(0.001)).setScale(2, RoundingMode.HALF_UP)
          } else {
            sumGhf2 = sumCjsl.*(BigDecimal(rateGH)).*(BigDecimal(rateGhzk)).setScale(2, RoundingMode.HALF_UP)
          }
        }
        //同一个申请编号总的佣金 （按申请编号汇总）
        var sumYj2 = sumCjje.*(BigDecimal(rateYJ)).*(BigDecimal(rateYjzk)).setScale(2, RoundingMode.HALF_UP)
        if (NO.equals(cs1)) {
          sumYj2 = sumYj2 - sumJsf2 - sumZgf2
        }
        if (YES.equals(cs4)) {
          sumYj2 = sumYj2 - sumFxj2
        }

        if (YES.equals(cs5)) {
          sumYj2 = sumYj2 - otherFee
        }

        if (sumYj2 < BigDecimal(minYj)) {
          sumYj2 = BigDecimal(minYj)
        }

        (bcrq + SEPARATE1 + zqdm + SEPARATE1 + gsdm + SEPARATE1 + bs,
          ShghFee("2", sumCjje, sumCjsl, sumYj2, sumJsf2, sumYhs2, sumZgf2,
            sumGhf2, sumFxj2))
      }
    }.groupByKey().map {
      case (key, fees) =>
        var totalYj2 = BigDecimal(0)
        var totalJsf2 = BigDecimal(0)
        var totalYhs2 = BigDecimal(0)
        var totalZgf2 = BigDecimal(0)
        var totalGhf2 = BigDecimal(0)
        var totalFxj2 = BigDecimal(0)
        var totalCjje = BigDecimal(0)
        var totalCjsl = BigDecimal(0)
        for (fee <- fees) {
          totalCjje += fee.sumCjje
          totalCjsl += fee.sumCjsl
          totalYj2 += fee.sumYj
          totalJsf2 += fee.sumJsf
          totalYhs2 += fee.sumYhs
          totalZgf2 += fee.sumZgf
          totalGhf2 += fee.sumGhf
          totalFxj2 += fee.sumFxj
        }
        (key, ShghFee("2", totalCjje, totalCjsl, totalYj2, totalJsf2, totalYhs2, totalZgf2,
          totalGhf2, totalFxj2))
    }

    //第三种 金额汇总*费率
    val fee3 = value1.map {
      case (key, values) => {
        val fields = key.split(SEPARATE1)
        val bs = fields(3) //买卖方向
        val gsdm = fields(2) //交易席位
        val bcrq = fields(0) //本次日期
        val zqdm = fields(1) //证券代码

        val getRateResult = getRate(gsdm, bcrq, ZCLB, ZCLB, ZYZCH, GYZCH)
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

        val otherFee = BigDecimal(0)
        var sumCjje = BigDecimal(0) //同一个申请编号总金额
        var sumCjsl = BigDecimal(0) //同一个申请编号总数量

        val csResults = getGgcs("117")
        val cs1 = csResults._1
        var cs2 = csResults._2
        val cs3 = csResults._3
        val cs4 = csResults._4
        val cs5 = csResults._5

        for (row <- values) {
          val cjje = BigDecimal(row.getAs[String]("CJJE"))
          val cjsl = BigDecimal(row.getAs[String]("CJSL"))
          sumCjje = sumCjje.+(cjje)
          sumCjsl = sumCjsl.+(cjsl)
        }

        var sumJsf2 = sumCjje.*(BigDecimal(rateJS)).*(BigDecimal(rateJszk)).setScale(2, RoundingMode.HALF_UP)
        //同一个申请编号总的经手费
        var sumYhs2 = BigDecimal(0) //同一个申请编号总的印花税
        if (SALE.equals(bs)) {
          sumYhs2 = sumCjje.*(BigDecimal(rateYH)).*(BigDecimal(rateYhzk)).setScale(2, RoundingMode.HALF_UP)
        }
        val sumZgf2 = sumCjje.*(BigDecimal(rateZG)).*(BigDecimal(rateZgzk)).setScale(2, RoundingMode.HALF_UP) //同一个申请编号总的征管费
        val sumFxj2 = sumCjje.*(BigDecimal(rateFXJ)).*(BigDecimal(rateFxjzk)).setScale(2, RoundingMode.HALF_UP) //同一个申请编号总的风险金
        //同一个申请编号总的过户费
        var sumGhf2 = BigDecimal(0)
        if (!(NO.equals(cs2) || (YES).equals(cs2))) {
          //如果时日期格式的话要比较日期 TODO日期格式的比较
          if (bcrq.compareTo(cs2) > 0) {
            cs2 = YES
          } else {
            cs2 = NO
          }
        }
        if (YES.equals(cs2)) {
          if (YES.equals(cs3)) {
            sumGhf2 = sumCjje.*(sumCjsl).*(BigDecimal(0.001)).setScale(2, RoundingMode.HALF_UP)
          } else {
            sumGhf2 = sumCjje.*(sumCjsl).*(BigDecimal(rateGH)).*(BigDecimal(rateGhzk)).setScale(2, RoundingMode.HALF_UP)
          }
        } else {
          if (YES.equals(cs3)) {
            sumGhf2 = sumCjsl.*(BigDecimal(0.001)).setScale(2, RoundingMode.HALF_UP)
          } else {
            sumGhf2 = sumCjsl.*(BigDecimal(rateGH)).*(BigDecimal(rateGhzk)).setScale(2, RoundingMode.HALF_UP)
          }
        }
        //同一个申请编号总的佣金 （按申请编号汇总）
        var sumYj2 = sumCjje.*(BigDecimal(rateYJ)).*(BigDecimal(rateYjzk)).setScale(2, RoundingMode.HALF_UP)
        if (NO.equals(cs1)) {
          sumYj2 = sumYj2 - sumJsf2 - sumZgf2
        }
        if (YES.equals(cs4)) {
          sumYj2 = sumYj2 - sumFxj2
        }

        if (YES.equals(cs5)) {
          sumYj2 = sumYj2 - otherFee
        }

        if (sumYj2 < BigDecimal(minYj)) {
          sumYj2 = BigDecimal(minYj)
        }

        (key, ShghFee("3", sumCjje, sumCjsl, sumYj2, sumJsf2, sumYhs2, sumZgf2,
          sumGhf2, sumFxj2))
      }
    }

    //将三种结果串联起来
    val middle = fee1.join(fee2).join(fee3)

    //最终结果
    val result = middle.map {
      case (key, ((fee1, fee2), fee3)) =>
        val fields = key.split(SEPARATE1)
        val bs = fields(3)
        var totalCjje = fee1.sumCjje
        var totalCjsl = fee1.sumCjsl

        var realYj = BigDecimal(0)
        var realJsf = BigDecimal(0)
        var realYhs = BigDecimal(0)
        var realZgf = BigDecimal(0)
        var realGhf = BigDecimal(0)
        var realFxj = BigDecimal(0)

        val jsResult = getJsgz("117")
        val con8 = jsResult._1
        val con1 = jsResult._2
        val con2 = jsResult._3
        val con3 = jsResult._4
        val con4 = jsResult._5
        val con5 = jsResult._6
        val con6 = jsResult._7
        val con11 = jsResult._8
        val con12 = jsResult._9
        val con13 = jsResult._10
        val con14 = jsResult._11
        val con15 = jsResult._12
        val con16 = jsResult._13

        //判断取值逻辑
        if (YES.equals(con1)) {
          realJsf = fee2.sumJsf
        } else if (YES.equals(con11)) {
          realJsf = fee1.sumJsf
        } else {
          realJsf = fee1.sumJsf
        }


        if (YES.equals(con2)) {
          realZgf = fee2.sumZgf
        } else if (YES.equals(con12)) {
          realZgf = fee2.sumZgf
        } else {
          realZgf = fee1.sumZgf
        }

        if (YES.equals(con3)) {
          realGhf = fee2.sumGhf
        } else if (YES.equals(con13)) {
          realGhf = fee1.sumGhf
        } else {
          realGhf = fee1.sumGhf
        }

        if (YES.equals(con4)) {
          realYhs = fee2.sumYhs
        } else if (YES.equals(con14)) {
          realYhs = fee1.sumYhs
        } else {
          realYhs = fee1.sumYhs
        }
        if (YES.equals(con5)) {
          realYj = fee2.sumYj
        } else if (YES.equals(con15)) {
          realYj = fee1.sumYj
        } else {
          realYj = fee1.sumYj
        }

        if (YES.equals(con6)) {
          realFxj = fee2.sumFxj
        } else if (YES.equals(con16)) {
          realFxj = fee1.sumFxj
        } else {
          realFxj = fee1.sumFxj
        }

        var FBje = BigDecimal(0)
        var FSje = BigDecimal(0)
        var FBsl = BigDecimal(0)
        var FSsl = BigDecimal(0)
        var FByj = BigDecimal(0)
        var FSyj = BigDecimal(0)
        var FBjsf = BigDecimal(0)
        var FSjsf = BigDecimal(0)
        var FByhs = BigDecimal(0)
        var FSyhs = BigDecimal(0)
        var FBzgf = BigDecimal(0)
        var FSzgf = BigDecimal(0)
        var FBghf = BigDecimal(0)
        var FSghf = BigDecimal(0)
        val FBgzlx = BigDecimal(0)
        val FSgzlx = BigDecimal(0)
        var FBFxj = BigDecimal(0)
        var FSFxj = BigDecimal(0)

        if (BUY.equals(bs)) {
          FBje = totalCjje
          FBsl = totalCjsl
          FBjsf = realJsf
          //          FByhs = sumYhs.setScale(2, RoundingMode.HALF_UP)
          FBzgf = realZgf
          FBghf = realGhf
          FBFxj = realFxj
          FByj = realYj
        } else {
          FSje = totalCjje
          FSsl = totalCjsl
          FSjsf = realJsf
          FSyhs = realYhs
          FSzgf = realZgf
          FSghf = realGhf
          FSFxj = realFxj
          FSyj = realYj
        }
        val bcrq = fields(0)
        val FSzsh = SH
        val Fjyxwh = fields(2)
        var FBsfje = FBje.+(FBjsf).+(FBzgf).+(FBghf)
        var FSssje = FSje.-(FSjsf).-(FSzgf).-(FSghf).-(FSyhs)
        if (YES.equals(con8)) {
          FBsfje += FByj
          FSssje -= FByj
        }
        val FZqbz = ZCLB
        val FYwbz = "DZ"
        val FQsbz = "N"
        val FBQtf = BigDecimal(0)
        val FSQtf = BigDecimal(0)
        val ZqDm = fields(1)
        val FJyFS = "PT"
        val Fsh = "1"
        val Fzzr = " "
        val Fchk = " "
        val fzlh = "0"
        val ftzbz = ""
        val FBQsghf = BigDecimal(0)
        val FSQsghf = BigDecimal(0)
        val FGddm = ""
        val FHGGAIN = BigDecimal(0)

        //bcrq + SEPARATE1 + zqdm + SEPARATE1 + gsdm + SEPARATE1 + gddm + SEPARATE1 + bs,
        ShghDto(bcrq, bcrq, ZqDm, FSzsh, Fjyxwh, FBje.formatted("%.2f"), FSje.formatted("%.2f"), FBsl.formatted("%.2f"), FSsl.formatted("%.2f"), FByj.formatted("%.2f"),
          FSyj.formatted("%.2f"), FBjsf.formatted("%.2f"), FSjsf.formatted("%.2f"), FByhs.formatted("%.2f"), FSyhs.formatted("%.2f"), FBzgf.formatted("%.2f"), FSzgf.formatted("%.2f"), FBghf.formatted("%.2f"), FSghf.formatted("%.2f"), FBgzlx.formatted("%.2f"),
          FSgzlx.formatted("%.2f"), FBFxj.formatted("%.2f"), FSFxj.formatted("%.2f"), FBsfje.formatted("%.2f"), FSssje.formatted("%.2f"), FZqbz, FYwbz, FQsbz, FBQtf.formatted("%.2f"), FSQtf.formatted("%.2f"),
          ZqDm, FJyFS, Fsh, Fzzr, Fchk, fzlh, ftzbz, FBQsghf.formatted("%.2f"), FSQsghf.formatted("%.2f"), FGddm, FHGGAIN.formatted("%.2f"))
    }

    //    Util.outputMySql(result.toDF(), "SHGH2")
    spark.stop()
  }

}
