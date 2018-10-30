package com.spark
import java.io.File

import com.yss.spark.KafkaUtilsSpark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * created by zhangkai
  * 测试上海过户标签
  */

object SHGHBQ {
  val SEPARATE1 = "@"
  val SEPARATE2 = ","
  val SEPARATE3 = "-"
  val PREFIX = "/yss/guzhi/interface/"

  def main(args: Array[String]): Unit = {

    //running()

    // }

    // private def running(): Unit = {
    val spark = SparkSession.builder().appName("sparkDemo").master("local[*]").getOrCreate()

    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))


    /**
      * 读取基金信息表
      */
    // val csjjxx = sc.textFile("E:\\WXWork Files\\File\\2018-09\\etl\\data\\CSJJXX_201809051144.csv")
    //csjjxx
    val csjjxxPath = Util.getDailyInputFilePath("CSJJXX")
    val csjjxx: RDD[(String, (String, String))] = sc.textFile(csjjxxPath)
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
      val FZQDMETF0 = fields(3)
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
    val csjjxx00Map = csjjxx.map(row => {
      val fields = row._2._2.split(SEPARATE2)
      val FZQLX = fields(9)
      val FZQDMETF0 = fields(2)
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
    val csjjxxMap = Map((1, csjjxx01Map), (2, csjjxx02Map), (5, csjjxx05Map), (0, csjjxx00Map), (3, csjjxx03Map), (4, csjjxx04Map), (6, csjjxx01HPMap))
    val csjjxxMapValue = sc.broadcast(csjjxxMap)

    /**
      * 读取 特殊科目设置表CsTsKm
      */
    val csTsKmPath = Util.getDailyInputFilePath("A001CSTSKM")
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
    val csTsKmValue = sc.broadcast(csTsKm)

    /**
      * 读取席位表CsqsXw表
      *
      **/

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
    val csqsxwValue = sc.broadcast(csqsxw)

    /**
      * 加载配股股票并广播
      */
    val csqyxxPath = Util.getDailyInputFilePath("CSQYXX")
    val csqyxx = sc.textFile(csqyxxPath)

    val csqyxMap = csqyxx
      .filter(row => {

        val str = row.split(",")
        val fqylx = str(1)
        val fsh = str(7)
        if ("PG".equals(fqylx) && "1".equals(fsh)) true
        else false
      })
      .map(row => {
        val strs = row.split(",")
        val fzqdm = strs(0)
        (fzqdm, row)
      })
      .groupByKey()
      .collectAsMap()
    val csqyxMapValue = sc.broadcast(csqyxMap)

    /**
      * 获取基金类型
      */
    val lsetcssysjjPath = Util.getDailyInputFilePath("LSETCSSYSJJ")
    val lsetcssysjj = sc.textFile(lsetcssysjjPath)
    val lsetcssysjjMap = lsetcssysjj
      .map(row => {
        val fields = row.split(SEPARATE2)
        (fields(0), fields(1) + SEPARATE1 + fields(3))
      })
      .collectAsMap()
    val lsetcssysjjValue = sc.broadcast(lsetcssysjjMap)

    /** *
      *
      * 加载公共参数表lvarlist
      * */

    val csbPath = Util.getDailyInputFilePath("LVARLIST")
    val csb = sc.textFile(csbPath)

    //将参数表转换成map结构
    val csbMap = csb.map(row => {
      val fields = row.split(SEPARATE2)
      val key = fields(0)
      val value = fields(1)
      (key, value)
    })
      .collectAsMap()
    val csbMapValue = sc.broadcast(csbMap)

    /**
      * 债券信息表 CSZQXX
      */
    val cszqxxPath = Util.getDailyInputFilePath("CSZQXX")
    val cszqxx = sc.textFile(cszqxxPath)
    //则获取CsZqXx表中fzqdm=gh文件中的zqdm字段的“FZQLX”字段值
    val cszqxxMap1 = cszqxx
      .map(row => {
        val fields = row.split(SEPARATE2)
        val zqdm = fields(0)
        val fzqlx = fields(11)
        (zqdm, fzqlx)
      }).collectAsMap()
    val cszqxxValue1 = sc.broadcast(cszqxxMap1)

    val cszqxxMap2 = cszqxx.filter(row => {
      val fields = row.split(SEPARATE2)
      val zqdm = fields(0)
      val fzqlb = fields(11)
      val fjysc = fields(12)
      val fsh = fields(19)
      val fjjdm = fields(2)
      val fstartdate = fields(22)
      if (!"银行间".equals(fjysc) && "可分离债券".equals(fzqlb)
        && "1".equals(fsh)) true
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
    val cszqxxValue2 = sc.broadcast(cszqxxMap2)

    /**
      * 股东账号表csgdzh
      *
      **/
    val csgdzhPath = Util.getDailyInputFilePath("CSGDZH")
    val csgdzhMap = sc.textFile(csgdzhPath)
      .map(row => {
        val fields = row.split(SEPARATE2)
        (fields(0), fields(5))
      }).collectAsMap()

    val csgdzhValue = sc.broadcast(csgdzhMap)

    /**
      * 判断是否要查询CsZqXx中ZQ的业务类型
      *
      * @param tzh  套账号
      * @param bcrq 日期
      * @param zqdm 证券代码
      * @return
      */
    def zqlx(tzh: String, bcrq: String, zqdm: String): Boolean = {
      val condition1 = csbMapValue.value.get(tzh + "债券类型取债券品种信息维护的债券类型")
      if (condition1.isDefined && "1".equals(condition1.get)) {
        val condition2 = csbMapValue.value.get(tzh + "债券类型取债券品种信息维护的债券类型启用日期")
        if (condition2.isDefined && bcrq.compareTo(condition2.get) >= 0) {
          val condition3 = csbMapValue.value.get(tzh + "债券类型取债券品种信息维护的债券类型代码段")
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
      * 判断是否是配股股票
      * select 1 from csqyxx where fqylx='PG' and fqydjr<=读数日期 and fjkjzr>=读数日期 and fstartdate<=读数日期 and fqybl not in('银行间','上交所','深交所','场外') and fsh=1 and fzqdm='gh文件中的zqdm'
      *
      * @param zqdm 证券代码
      * @param bcrq 读数日期
      * @return 是否
      */
    def sfspggp(zqdm: String, bcrq: String): Boolean = {
      val maybeRows = csqyxMapValue.value.get(zqdm)
      if (maybeRows.isDefined) {
        val condition1 = Array("银行间", "上交所", "深交所", "场外")
        for (row <- maybeRows.get) {
          val str = row.split(",")
          val fqydjr = str(4)
          val fjkjzr = str(6)
          val fstartdate = str(11)
          val fqybl = str(2)
          if (bcrq.compareTo(fqydjr) >= 0
            && bcrq.compareTo(fjkjzr) <= 0
            && bcrq.compareTo(fstartdate) >= 0
            && !condition1.contains(fqybl)) return true
        }
      }
      return false
    }

    /**
      * 判断基金信息表维护 FZQDMETF0、1 、2、3、4、5=该zqdm
      *
      * @param fzqlx 目前基金类型为ETF、HB
      * @param zqdm
      * @param bcrq
      * @param flag
      * @return
      */
    def jjxxbwh(fzqlx: String, zqdm: String, bcrq: String, flag: Int): Boolean = {
      val map = csjjxxMapValue.value(flag)
      val maybeString = map.get(fzqlx + SEPARATE2 + zqdm)
      if (maybeString.isDefined) {
        if (bcrq.compareTo(maybeString.get) >= 0) return true
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
      val maybe = csgdzhValue.value.get(gddm)
      if (maybe.isEmpty) {
        throw new Exception("未找到对应的套账号：" + gddm)
      }
      maybe.get
    }

    /**
      * 进行日期的格式化，基础信息表日期是 yyyy-MM-dd,原始数据是 yyyyMMdd
      * 这里将原始数据转换成yyyy-MM-dd的格式
      *
      * @param bcrq
      * @return yyyy-MM-dd
      */
    def convertDate(bcrq: String) = {
      val yyyy = bcrq.substring(0, 4)
      val mm = bcrq.substring(4, 6)
      val dd = bcrq.substring(6)
      yyyy.concat(SEPARATE3).concat(mm).concat(SEPARATE3).concat(dd)
    }

    /**
      * ETF基金需要获取文件的中sqbh,在计算业务标识时候需要用到
      */
    val fileName = args(0)
    val sourcePath = Util.getInputFilePath(fileName, PREFIX)
    val df = Util.readCSV(sourcePath, spark)
    val sqbhs = df.rdd.filter(row => {
      val str = row.toString().split("\t")
      var bcrq = str(2)
      bcrq = convertDate(bcrq)
      var zqdm = str(7)
      val cjjg = str(10)
      if (zqdm.startsWith("5")) {
        if (cjjg.equals("0")) {
          if (jjxxbwh("ETF", zqdm, bcrq, 0)) true
          else false
        } else false
      } else false
    }).map(row => {
      row.getAs[String](12)
    }) collect()
    val sqbhValues = sc.broadcast(sqbhs)

    /**
      * 一、获取证券标志
      *
      * @param zqdm 证券代码
      * @param cjjg 成交价格
      * @param bcrq 本次日期
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
      if (zqdm.startsWith("742")) return "QY"
      if (zqdm.startsWith("714") || zqdm.startsWith("760") || zqdm.startsWith("781")) {
        if (sfspggp(zqdm, bcrq)) return "QY"
        return "XG"
      }
      if (zqdm.startsWith("73")) return "XG"
      if (zqdm.startsWith("70")) {
        if ("100".equals(cjjg)) return "XZ"
        if (sfspggp(zqdm, bcrq)) return "QY"
        return "XG"
      }
      throw new Exception("无法找到对应的证券标志：" + zqdm)
    }

    /**
      * 二、获取业务标志
      *
      * @param tzh  套账号
      * @param zqbz 证券标志
      * @param zqdm 证券代码
      * @param cjjg 成交价格
      * @param bs   买卖
      * @param gsdm 公司代码
      * @param bcrq 本次日期
      * @return
      */
    def getYwbz(tzh: String, zqbz: String, zqdm: String, cjjg: String, bs: String, gsdm: String, bcrq: String, sqbh: String): String = {
      if ("GP".equals(zqbz) || "CDRGP".equals(zqbz)) {
        val condition = csTsKmValue.value.get(tzh + "指数、指标股票按特殊科目设置页面处理")
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
        val lset = lsetcssysjjValue.value.getOrElse(tzh, "-1@-1").split("@")
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
        if (sqbhValues.value.contains(sqbh) && (jjxxbwh("ETF", zqdm, bcrq, 2) || jjxxbwh("ETF", zqdm, bcrq, 5))) {
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
          val condition = cszqxxValue1.value.get(zqdm)
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
          var res = cszqxxValue2.value.get(zqdm + SEPARATE1 + tzh)
          if (res.isDefined && bcrq.compareTo(res.get) >= 0) {
            return "FLKZZ"
          } else {
            res = cszqxxValue2.value.get(zqdm + SEPARATE1 + " ")
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
          val lset = lsetcssysjjValue.value.getOrElse(tzh, "-1@-1").split("@")
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
          val lset = lsetcssysjjValue.value.getOrElse(tzh, "-1@-1").split("@")
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

    val Dstream = KafkaUtilsSpark.getStream(ssc)

    Dstream.filter(x => x.currentRecord != 1).filter(x => x.currentRecord != 0).filter(f => { //过滤表头
      //      println(f.rowValue) //D890026748,,20180809,2116746,23341,5700,0,600271,100151,100151,26.930,153501.00,0000001149,B,00001
      new File(f.fileName).getName.equalsIgnoreCase("shghtest")
    }).foreachRDD(rdd => {
      rdd.foreach(f => {
        val v = f.rowValue.split("\n")
        v.foreach(record => {
          val str = record.split(",")
          val gddm = str(0)
          val gdxm = str(1)
          val bcrq = str(2) //本次日期
          val cjbh = str(3)
          val gsdm = str(4) // 公司代码
          val cjsl = str(5)
          val bcye = str(6)
          val zqdm = str(7) //证券代码
          val sbsj = str(8)
          val cjsj = str(9)
          val cjjg = str(10) //成交价格
          val cjje = str(11)
          val sqbh = str(12)
          val bs = str(13)
          val mjbh = str(14)
          val zqbz = getZqbz(zqdm, cjjg, bcrq)
          val tzh = getTzh(gddm)
          val ywbz = getYwbz(tzh, zqbz, zqdm, cjjg, bs, gsdm, bcrq, sqbh)
          println(gddm, gdxm, bcrq, cjbh, gsdm, cjsl, bcye, zqdm, sbsj, cjsj, cjjg, cjje, sqbh, bs, mjbh, zqbz, ywbz)
        })

      })

    })


    ssc.start()
    ssc.awaitTermination()

    //}
  }
}
