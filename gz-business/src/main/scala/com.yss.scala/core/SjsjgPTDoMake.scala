package com.yss.scala.core

import com.yss.scala.dto._
import com.yss.scala.core.SjsjgContants._
import com.yss.scala.util.{DateUtils, BasicUtils}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.math.BigDecimal.RoundingMode

/**
  * @author erwa he
  * @version 2018-11-08
  *          describe:
  *          目标文件：sjsjg文件
  *          目标表：
  */
object SjsjgPTDoMake {

  def main(args: Array[String]): Unit = {
    var ywrq = DateUtils.getToday(DateUtils.YYYYMMDD)
    if(args.length > 1){
      ywrq = args(0)
    }
    execute(ywrq)
  }

  def execute(findate:String) = {
    val spark = SparkSession.builder().appName("SjsjgPTDoMake").master("local[*]").getOrCreate()
    val lvarlist = loadLvarlist(spark.sparkContext,findate)
    val df = doMake(spark, lvarlist,findate)
    import spark.implicits._
    doSum(spark, df.toDF(), lvarlist,findate)
    spark.stop()
  }

  /**将yyyyMMdd日期格式转换成yyyy-MM-dd的格式 */
  def convertDate(bcrq:String) = {
    val yyyy = bcrq.substring(0,4)
    val mm = bcrq.substring(4,6)
    val dd = bcrq.substring(6)
    yyyy.concat(SEPARATE3).concat(mm).concat(SEPARATE3).concat(dd)
  }

  /** * 加载公共参数表lvarlist */
  def loadLvarlist(sc: SparkContext, ywrq:String) = {

    val convertedFindate = convertDate(ywrq)
    val csbPath = BasicUtils.getDailyInputFilePath(TABLE_NAME_GGCS)
    val csb = sc.textFile(csbPath)

    //将参数表转换成map结构
    val csbMap = csb
      .filter(row => {
        val fields = row.split(SEPARATE2)
        if(fields(5).compareTo(convertedFindate)<=0) true else false
      })
      .map(row => {
        val fields = row.split(SEPARATE2)
        val key = fields(0)
        val value = fields(1)
        (key, value)
      })
      .collectAsMap()
    sc.broadcast(csbMap)
  }

  /** 加载各种基础信息表，并广播 */
  def loadTables(spark: SparkSession, ywrq: String) = {

    val sc = spark.sparkContext

    /** * 读取基金信息表csjjxx */
    def loadCsjjxx() = {
      val csjjxxPath = BasicUtils.getDailyInputFilePath(TABLE_NAME_JJXX)
      val csjjxx = sc.textFile(csjjxxPath)
        .filter(row => {
          val fields = row.split(SEPARATE2)
          val fsh = fields(10)
          val fszsh = fields(8)
          val fstartdate = fields(14)
          if (FSH.equals(fsh) && "S".equals(fszsh) && fstartdate.compareTo(ywrq) <= 0) true
          else false
        })
        .map(row => {
          val fields = row.split(SEPARATE2)
          val zqdm = fields(1) //证券代码
          val fzqlx = fields(9) //证券类型
          (zqdm + SEPARATE1 + fzqlx)
        }).collect()

      sc.broadcast(csjjxx)
    }

    /** 债券信息表cszqxx */
    def loadCszqxx() = {
      //读取CsZqXx表
      val cszqxxPath = BasicUtils.getDailyInputFilePath(TABLE_NAME_ZQXX)
      val cszqxx = sc.textFile(cszqxxPath)
      val cszqxxMap1 = cszqxx
        .map(row => {
          val fields = row.split(SEPARATE2)
          val zqdm = fields(0)
          val fzqlx = fields(11)
          (zqdm, fzqlx)
        }).collectAsMap()

      val cszqxxArray = cszqxx.filter(row => {
        val fields = row.split(SEPARATE2)
        val zqdm = fields(0)
        val fzqlb = fields(11)
        val fjysc = fields(12)
        val fsh = fields(19)
        val fjjdm = fields(2)
        val fstartdate = fields(22)
        if (!"银行间".equals(fjysc) && "可分离债券".equals(fzqlb)
          && FSH.equals(fsh) && fstartdate.compareTo(ywrq) <= 0
        ) true
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
          zqdm + SEPARATE1 + fjjdm
        })
        .collect()
      (sc.broadcast(cszqxxMap1), sc.broadcast(cszqxxArray))
    }

    /** 股东账号表csgdzh */
    def loadCsgdzh() = {
      //读取股东账号表，
      val csgdzhPath = BasicUtils.getDailyInputFilePath(TABLE_NAME_GDZH)
      val csgdzhMap = sc.textFile(csgdzhPath)
        .map(row => {
          val fields = row.split(SEPARATE2)
          (fields(0), fields(5))
        }).collectAsMap()

      sc.broadcast(csgdzhMap)
    }

    /** 已计提国债利息 JJGZLX */
    def loadGzlx() = {
      val jjgzlxPath = BasicUtils.getDailyInputFilePath(TABLE_NAME_GZLX)
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
      val csholidayPath = BasicUtils.getDailyInputFilePath(TABLE_NAME_HOLIDAY)
      val csholidayList = sc.textFile(csholidayPath)
        .filter(str => {
          val fields = str.split(SEPARATE2)
          val fdate = fields(0)
          val fbz = fields(1)
          val fsh = fields(3)
          if (DEFAULT_VALUE_0.equals(fbz) && FSH.equals(fsh) && fdate.compareTo(ywrq) >= 0) true
          else false
        })
        .map(str => {
          str.split(SEPARATE2)(0)
        }).takeOrdered(1)
      if (csholidayList.length == 0) ywrq
      else csholidayList(0)
    }

    /** 加载资产信息表 lsetlist */
    def loadLsetlist() = {
      val lsetlistPath = BasicUtils.getDailyInputFilePath(TABLE_NAME_ZCXX)
      val lsetlistMap = sc.textFile(lsetlistPath)
        .map(row => {
          val fields = row.split(SEPARATE2)
          val fsetid = fields(1) // 资产代码
          val fsetcode = fields(2) //套账号
          (fsetcode,fsetid)  //根据套账号获取资产代码
        })
        .collectAsMap()
      sc.broadcast(lsetlistMap)
    }
    (loadCsjjxx(),loadCszqxx(), loadCsgdzh(), loadGzlx(), loadCsholiday(),loadLsetlist())
  }

  /** 进行原始数据的转换*/
  def doMake(spark: SparkSession, csb: Broadcast[collection.Map[String, String]], ywrq:String) = {
    val sc = spark.sparkContext
    // 读取原始数据
    val sourcePath = BasicUtils.getInputFilePath("/SJSJG"+ywrq.substring(4))
    val df = BasicUtils.readCSV(sourcePath,spark)
    // 转换日期
    val convertedYwrq = convertDate(ywrq)
    // 加载基础班信息
    val broadcaseValues = loadTables(spark, convertedYwrq)
    val csbValues = csb
    val csjjxxValues = broadcaseValues._1
    val cszqxxValues = broadcaseValues._2
    val csgdzhValues = broadcaseValues._3
    val gzlxValues = broadcaseValues._4
    val csholiday = broadcaseValues._5
    val lsetlistValues = broadcaseValues._6


    /**
      * 计算国债利息
      * @param zqbz 证券标志
      * @param zqdm 证券代码
      * @param cjsl 成交数量
      * @return
      */
    def gzlx(zqbz: String, zqdm: String, cjsl: String): String = {
      var gzlx = DEFAULT_VALUE_0
      if ("ZQ".equals(zqbz)) {
        val may = gzlxValues.value.get(zqdm + SEPARATE1 + convertedYwrq)
        if (may.isDefined) {
          gzlx = BigDecimal(may.get).*(BigDecimal(cjsl)).*(BigDecimal(10)).setScale(DEFAULT_DIGIT, RoundingMode.HALF_UP).formatted(DEFAULT_DIGIT_FORMAT)
        }
      }
      gzlx
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
      * 获取证券标志
      *
      * @param zqdm 证券代码
      * @param jgywlb 业务类别
      * @param jgsfje 金额
      * @param jgjssl 数量
      * @param fsetcode 套账号
      * @return
      *
      *业务类别:
      *   QPPX	股息派发
      *   PGQZ	配股权证到帐
      *   PGRG	配股认购
      *   ZTZC	转托管业务中转出托管单元的记录
      *   ZTZR	转托管业务中转入托管单元的记录
      *   ZQZG	报盘转股及强制转股
      *   ZQSH  债券赎回
      *   QZ02  以证券给付结算的认沽权证交收回报
      *   QZ04  以证券给付结算的认购权证交收回报
      *   FJZR  非交易转让之费用记录
      *   TA4A  基金盘后业务（资券通申购赎回）
      *   GS4B  证券分拆、折算持有数量变更
      *   QPHG  红股/转增股到账
      *   YYGH  要约收购股份过户记录
      *   TGXG  IPO新股登记
      *   ZYWJ
      */
    def getZqbz(zqdm: String, jgywlb: String, jgsfje: Int, jgjssl: Int,fsetcode: String): String = {

      val isFlag =  csbValues.value.getOrElse(fsetcode + CON20_KEY,"")  //深圳交易数据来源

      val isFlagValue = csbValues.value.getOrElse(fsetcode + CON20_KEY_1,"") //0: 深交所回报数据  1：深圳中登结算数据  2：深交所V5接口XML文件 3：深交所V5接口数据库直连 4：深交所V5接口成交汇总

      if (zqdm.startsWith("16") && "TGXG".equalsIgnoreCase(jgywlb))   "JJ"
      else if ("ZQZG".equalsIgnoreCase(jgywlb)){
        if (zqdm.startsWith("12"))  "ZQ"
         "GP"
      }
      else if ("PGQZ".equalsIgnoreCase(jgywlb) || "QPHG".equalsIgnoreCase(jgywlb) || "ZYWJ".equalsIgnoreCase(jgywlb))  "QY"
      else if ("ZQSH".equalsIgnoreCase(jgywlb)) "ZQ"
      else if ("FJZR".equalsIgnoreCase(jgywlb) || "YYGH".equalsIgnoreCase(jgywlb))  "GP"
      else if (("QZ02".equalsIgnoreCase(jgywlb) || "QZ04".equalsIgnoreCase(jgywlb))&& jgsfje != 0 &&  jgjssl !=0 )  "QZ"
      else if ("GS4B".equalsIgnoreCase(jgywlb)) "JJFECF"   //需要增加 “cscnjjzh”表 hew
      else if ("TA4A".equalsIgnoreCase(jgywlb))  "JJ"
      else if ("QPPX".equalsIgnoreCase(jgywlb) && (jgsfje > 0 &&  jgjssl ==0 || jgsfje == 0 &&  jgjssl > 0 ) )  "QY"
      else if ("PGRG".equalsIgnoreCase(jgywlb) && "1".equals(isFlag) && "1".equals(isFlagValue) && jgjssl!=0 && (jgsfje/jgjssl==100) )  "XZ"
      else if ("PGRG".equalsIgnoreCase(jgywlb) && "1".equals(isFlag) && "1".equals(isFlagValue) && (jgjssl==0 ||(jgjssl!=0  && (jgsfje/jgjssl!=100))) )  "QY"
      else if ((zqdm.startsWith("16")||zqdm.startsWith("15")) && ("ZTZC".equalsIgnoreCase(jgywlb)||"ZTZR".equalsIgnoreCase(jgywlb)))   "ZTGYW"
      else DEFAULT_VALUE
    }

    /**
      * 获取业务标识
      * @param zqdm 证券代码
      * @param jgywlb 业务类别
      * @param jgsfje 金额
      * @param jgjssl 数量
      * @param fsetcode 套账号
      * @return
      */
    def getYwbz(zqdm: String,jgywlb: String, jgsfje: Int, jgjssl: Int,fsetcode: String): String = {

      val booZqlb = csjjxxValues.value.contains(zqdm + SEPARATE1 + "FBS")
      val booZqlbLof = csjjxxValues.value.contains(zqdm + SEPARATE1 + "LOF")
      val booZqlbEtf = csjjxxValues.value.contains(zqdm + SEPARATE1 + "ETF")
      var reFlag1 = cszqxxValues._2.value.contains(zqdm + SEPARATE1 + fsetcode)
      var reFlag2 = cszqxxValues._2.value.contains(zqdm + SEPARATE1 + " ")
      val isFlag = csbValues.value.getOrElse(fsetcode + CON20_KEY,"")
      val isFlagValue = csbValues.value.getOrElse(fsetcode + CON20_KEY_1,"")

      if (zqdm.startsWith("16") && "TGXG".equalsIgnoreCase(jgywlb))   "LOFRGQR"
      else if ("ZQZG".equalsIgnoreCase(jgywlb))  "KZZGP"
      else if ("PGQZ".equalsIgnoreCase(jgywlb))  "QZ"
      else if ("ZQSH".equalsIgnoreCase(jgywlb) && zqdm.startsWith("11") && (reFlag1 || reFlag2 ))  "FLKZZSH"
      else if ("ZQSH".equalsIgnoreCase(jgywlb) && zqdm.startsWith("11"))  "QYZQSH"
      else if ("ZQSH".equalsIgnoreCase(jgywlb) )  "KZZSH"
      else if ("FJZR".equalsIgnoreCase(jgywlb) || "YYGH".equalsIgnoreCase(jgywlb))  "YYSell"
      else if (("QZ02".equalsIgnoreCase(jgywlb) || "QZ04".equalsIgnoreCase(jgywlb))&& jgsfje != 0 &&  jgjssl !=0 ){
        if(jgsfje > 0){
           "RZQZXQ"
        }else{
           "RGQZXQ"
        }
      }
      else if ("GS4B".equalsIgnoreCase(jgywlb)) {
        if(zqdm.startsWith("15") && booZqlb){
           "FBS"
        }else if(zqdm.startsWith("1599")){
           "ETF"
        }else if(zqdm.startsWith("15") && !booZqlb && !zqdm.startsWith("1599")){
           "LOF"
        }else if(zqdm.startsWith("16")){
           "LOF"
        }else if(zqdm.startsWith("18")){
           "JJ"
        }
      }
      else if ("TA4A".equalsIgnoreCase(jgywlb) )  "FJJJPDZH"
      else if ("ZYWJ".equalsIgnoreCase(jgywlb) )  "ZQPX"
      else if ("QPPX".equalsIgnoreCase(jgywlb) ){
        if((zqdm.startsWith("10")||zqdm.startsWith("11")||zqdm.startsWith("10")) && jgjssl==0 && jgsfje >0){
           "ZQPX"
        }
        if((zqdm.startsWith("15")||zqdm.startsWith("16")||zqdm.startsWith("18")) && jgjssl==0 && jgsfje >0){
           "JJPX"
        }
        if((!zqdm.startsWith("15") && !zqdm.startsWith("16") && !zqdm.startsWith("18") && !zqdm.startsWith("10") && !zqdm.startsWith("11") && !zqdm.startsWith("12")) && jgjssl==0 && jgsfje >0){
           "PX"
        }else{
           "SG"
        }

      }
      else if ("PGRG".equalsIgnoreCase(jgywlb) && "1".equals(isFlag) && "1".equals(isFlagValue) && jgjssl!=0 && (jgsfje/jgjssl==100) )  "KZZXZ"
      else if ("PGRG".equalsIgnoreCase(jgywlb) && "1".equals(isFlag) && "1".equals(isFlagValue) && (jgjssl==0 ||(jgjssl!=0  && (jgsfje/jgjssl!=100))) )  "PG"
      else if ((zqdm.startsWith("16")||zqdm.startsWith("15")) && "ZTZC".equalsIgnoreCase(jgywlb)){
        if(booZqlbLof){
           "LOFZC"
        }else if(booZqlbEtf){
           "ETFZC"
        }
      }
      else if ((zqdm.startsWith("16")||zqdm.startsWith("15")) && "ZTZR".equalsIgnoreCase(jgywlb)){
        if(booZqlbLof){
           "LOFZR"
        }else if(booZqlbEtf){
           "ETFZR"
        }
      }
      else if("QPHG".equalsIgnoreCase(jgywlb))   "SG"
      DEFAULT_VALUE
    }

    /**
      * 转换证券代码
      *
      * @param zqdm 证券代码
      * @return
      */
    def getZqdm(zqdm: String): String = {
      val end4 = zqdm.substring(2)
      val end3 = zqdm.substring(3)
      if (zqdm.startsWith("3"))  "30" + end4
      if (zqdm.startsWith("18"))  zqdm
      if (zqdm.startsWith("003"))  "600" + end3
      "00" + end4
    }


    /**
      * 中小企业板启用003000-004999代码段日期(YYYYMMDD)
      * @return
      */
    def getQYDate() = {
      csbValues.value.get(CON0S0_KEY)
    }

    /** 根据套账号获取资产代码 */
    def getFsetId(fsetcode:String) = {
      lsetlistValues.value.getOrElse(fsetcode,DEFAULT_VALUE)
    }

    // 向df原始数据中添加 fzqbz,fywbz,tzh,fgzlx,fsetid
    val etlRdd = df.rdd.map(row => {
      val jgjszh = row.getAs[String](0)
      val jgbfzh = row.getAs[String](1)
      val jgsjlx = row.getAs[String](2)
      val jgywlb = row.getAs[String](3)
      var jgzqdm = row.getAs[String](4)
      val jgjydy = row.getAs[String](5)
      val jgtgdy = row.getAs[String](6)
      val jgzqzh = row.getAs[String](7)
      val jgddbh = row.getAs[String](8)
      val jgyyb = row.getAs[String](9)
      val jgzxbh = row.getAs[String](10)
      val jgywlsh = row.getAs[String](11)
      val jgcjsl = row.getAs[String](12)
      val jgqssl = row.getAs[String](13)
      val jgjssl = row.getAs[String](14)
      val jgcjjg = row.getAs[String](15)
      val jgqsjg = row.getAs[String](16)
      val jgxyjy = row.getAs[String](17)
      val jgpcbs = row.getAs[String](18)
      val jgzqlb = row.getAs[String](19)
      val jgzqzl = row.getAs[String](20)
      val jggfxz = row.getAs[String](21)
      val jgltlx = row.getAs[String](22)
      val jgjsfs = row.getAs[String](23)
      val jghbdh = row.getAs[String](24)
      val jgqsbj = row.getAs[String](25)
      val jgyhs = row.getAs[String](26)
      val jgjyjsf = row.getAs[String](27)
      val jgjggf = row.getAs[String](28)
      val jgghf = row.getAs[String](29)
      val jgjsf = row.getAs[String](30)
      val jgsxf = row.getAs[String](31)
      val jgqsyj = row.getAs[String](32)
      val jgqtfy = row.getAs[String](33)
      val jgzjje = row.getAs[String](34)
      val jgsfje = row.getAs[String](35)
      val jgjsbz = row.getAs[String](36)
      val jgzydh = row.getAs[String](37)
      val jgcjrq = row.getAs[String](38)
      val jgqsrq = row.getAs[String](39)
      val jgjsrq = row.getAs[String](40)
      val jgfsrq = row.getAs[String](41)
      val jgqtrq = row.getAs[String](42)
      val jgscdm = row.getAs[String](43)
      val jgjyfs = row.getAs[String](44)
      val jgzqdm2 = row.getAs[String](45)
      val jgtgdy2 = row.getAs[String](46)
      val jgddbh2 = row.getAs[String](47)
      val jgfjsm = row.getAs[String](48)
      val jgbybz = row.getAs[String](49)
      jgzqdm = getZqdm(jgzqdm)
      val tzh =getTzh(jgzqzh)
      val fzqbz = getZqbz(jgzqdm, jgywlb, jgsfje.toInt, jgjssl.toInt,tzh)
      val fywbz = getYwbz(jgzqdm,jgywlb, jgsfje.toInt, jgjssl.toInt,tzh)
      val fgzlx = "0" // gzlx(fzqbz,jgzqdm,jgcjsl)
      val fsetid =getFsetId(tzh)
        SJSJGETL_PT(jgjszh,jgbfzh,jgsjlx,jgywlb,jgzqdm,jgjydy,jgtgdy,jgzqzh,jgddbh,jgyyb,jgzxbh,jgywlsh,jgcjsl,jgqssl,jgjssl,jgcjjg,jgqsjg,jgxyjy,jgpcbs,jgzqlb,jgzqzl,jggfxz,jgltlx,jgjsfs,jghbdh,jgqsbj,jgyhs,jgjyjsf,jgjggf,jgghf,jgjsf,jgsxf,jgqsyj,jgqtfy,jgzjje,jgsfje,jgjsbz,jgzydh,jgcjrq,jgqsrq,jgjsrq,jgfsrq,jgqtrq,jgscdm,jgjyfs,jgzqdm2,jgtgdy2,jgddbh2,jgfjsm,jgbybz,fzqbz,fywbz,tzh,fgzlx,fsetid)
    })
    etlRdd
  }

  /** 汇总然后进行计算 */
  def doSum(spark: SparkSession, df: DataFrame, csb: Broadcast[collection.Map[String, String]], ywrq:String) = {

    val sc = spark.sparkContext
    // 转换日期
    val convertedYwrq = convertDate(ywrq)
    /** 加载公共费率表和佣金表 */
    def loadFeeTables() = {
      //公共的费率表
      val flbPath = BasicUtils.getDailyInputFilePath(TATABLE_NAME_JYLV)
      val flb = sc.textFile(flbPath)
      //佣金利率表
      val yjPath = BasicUtils.getDailyInputFilePath(TABLE_NAME_A117CSJYLV)
      val yjb = sc.textFile(yjPath)

      //基金信息表csjjxx
      val csjjxxPath = BasicUtils.getDailyInputFilePath(TABLE_NAME_JJXX)
      val jjxxb = sc.textFile(csjjxxPath)

      //券商过户费承担方式
      val csqsfylvPath = BasicUtils.getDailyInputFilePath(TABLE_NAME_CSQSFYLV)
      val csqsfy = sc.textFile(csqsfylvPath)
      val csqsfyMap = csqsfy
        .filter(row => {
          val fields = row.split(SEPARATE2)
          val fsh = fields(13)
          val fstartdate = fields(16)
          if("1".equals(fsh) && fstartdate.compareTo(convertedYwrq) <= 0) true else false
        })
        .map(row => {
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
        })
        .collectAsMap()

      //将佣金表转换成map结构
      val yjbMap = yjb
        .filter(row => {
          val fields = row.split(SEPARATE2)
          val startDate = fields(16) //启用日期
          if(startDate.compareTo(convertedYwrq) <= 0) true else false
        })
        .map(row => {
          val fields = row.split(SEPARATE2)
          val fsetid = fields(1) //资产代码
          val zqlb = fields(3) //证券类别
          val sh = fields(4) //市场
          val lv = fields(5) //利率
          val minLv = fields(6) //最低利率
          val startDate = fields(16) //启用日期
          val zk = fields(12) //折扣
          val fstr1 = fields(8) //交易席位/公司代码
          val key = fsetid + SEPARATE1 + zqlb + SEPARATE1 + sh + SEPARATE1 + fstr1 //资产代码+证券类别+市场+交易席位/公司代码
          val value = startDate + SEPARATE1 + lv + SEPARATE1 + zk + SEPARATE1 + minLv //启用日期+利率+折扣+最低佣金值
          (key, value)
        })
        .groupByKey()
        .mapValues(arrs => {
          arrs.toArray.sortWith((str1, str2) => {
            str1.split(SEPARATE2)(0).compareTo(str2.split(SEPARATE2)(0)) > 0
          })(0)
        })
        .collectAsMap()

      //将费率表转换成map结构
      val flbMap = flb
        .filter(row =>{
          val fields = row.split(SEPARATE2)
          val startDate = fields(13)
          if(startDate.compareTo(convertedYwrq) <= 0) true else false
        })
        .map(row => {
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
        .mapValues(arrs => {
          arrs.toArray.sortWith((str1, str2) => {
            str1.split(SEPARATE2)(0).compareTo(str2.split(SEPARATE2)(0)) > 0
          })(0)
        })
        .collectAsMap()

      //过滤基金信息表
      val jjxxAarry = jjxxb
        .filter(row => {
          val fields = row.split(SEPARATE2)
          val fzqlx = fields(9)
          val ftzdx = fields(15)
          val fszsh = fields(8)
          val fstartdate = fields(14)
          if("ETF".equals(fzqlx) && SH.equals(fszsh) && "ZQ".equals(ftzdx)
            && fstartdate.compareTo(convertedYwrq) <= 0 )  true
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
      * 原始数据转换
      * key = 发送日期+证券代码+交易席位+股东代码+套账号+证券标志+业务标志+资产代码
      */
    val groupedRdd = df.rdd.map(row => {
      var gddmFlag = " " //交易席位
      val tempJgjydy = row.getAs[String]("JGJYDY")
      val tempJgjydy1 = row.getAs[String]("JGTGDY")
      if(!tempJgjydy.isEmpty){
        gddmFlag = tempJgjydy
      }else if(!tempJgjydy1.isEmpty){
        gddmFlag = tempJgjydy
      }else{
        gddmFlag = " "
      }
      val jgfsrq = row.getAs[String]("JGFSRQ") //发送日期
      val jgzqdm = row.getAs[String]("JGZQDM") //证券代码
      val jgzqzh = row.getAs[String]("JGZQZH") //股东代码
      val tzh = row.getAs[String]("TZH") //套账号
      val zqbz = row.getAs[String]("FZQBZ") //证券标志
      val ywbz = row.getAs[String]("FYWBZ") //业务标志
      val fsetid = row.getAs[String]("FSETID")
      val key = jgfsrq + SEPARATE1 + jgzqdm + SEPARATE1 + gddmFlag  + SEPARATE1 +
        jgzqzh + SEPARATE1 + tzh + SEPARATE1 + zqbz + SEPARATE1 + ywbz  + SEPARATE1 + fsetid
      (key, row)
    }).groupByKey()

    /**
      * 获取公共费率和佣金费率
      *
      * @param gsdm  交易席位
      * @param ywbz1  业务标识
      * @param zqbz1  证券标识
      * @param zyzch 专用资产号
      * @param gyzch 公用资产号
      * @return
      */
    def getRate( zqdm:String,gsdm: String, gddm: String,fsetid:String, ywbz1: String, zqbz1: String, zyzch: String, gyzch: String) = {
      //为了获取启动日期小于等于处理日期的参数
      val flbMap = flbValues.value
      val yjMap = yjbValues.value

      var ywbz = ywbz1
      var zqbz = zqbz1
      var sh = SH
      var jsf = JSF

      /**
        * 获取公共的费率
        * key = 证券类别+市场+资产号+利率类别
        * value = 启用日期+利率+折扣
        * 获取费率时先按照专用资产号取再用公用资产号取，先按照ywbz取再按照zqbz取
        */
      def getCommonFee(fllb: String,zqbz:String,ywbz:String,sh:String,zyzch:String,gyzch:String) =   {
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
        var maybeRateYJStr = yjMap.get(fsetid + SEPARATE1 + ywbz + SEPARATE1 + SH + SEPARATE1 + gsdm)
        if (maybeRateYJStr.isEmpty) {
          maybeRateYJStr = yjMap.get(fsetid + SEPARATE1 + ywbz + SEPARATE1 + SH + SEPARATE1 + gddm)
          if (maybeRateYJStr.isEmpty) {
            maybeRateYJStr = yjMap.get(fsetid + SEPARATE1 + zqbz + SEPARATE1 + SH + SEPARATE1 + gsdm)
            if (maybeRateYJStr.isEmpty) {
              maybeRateYJStr = yjMap.get(fsetid + SEPARATE1 + zqbz + SEPARATE1 + SH + SEPARATE1 + gddm)
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
      //计算佣金的费率承担模式
      (cs1, cs2, cs3, cs4, cs5,cs6,cs7)
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

    val feeAdd = groupedRdd.map  {
      case (key, values) =>
        val fields = key.split(SEPARATE1)
        //发送日期+证券代码+交易席位+股东代码+套账号+证券标志+业务标志+资产代码
        val bcrq = fields(0)
        val zqdm = fields(1)
        val gsdm = fields(2)
        val gddm = fields(3)
        val tzh = fields(4)
        val zqbz = fields(5)
        val ywbz = fields(6)
        val fsetid = fields(7)

        val getRateResult = getRate(zqdm,gsdm, gddm,fsetid, ywbz, zqbz, tzh, GYZCH)
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
          val cjje = BigDecimal(row.getAs[String]("JGSFJE"))
          val cjsl = BigDecimal(row.getAs[String]("JGJSSL"))
          val gzlx = BigDecimal(row.getAs[String]("FGZLX"))
          // 经手费的计算
          val jsf = cjje.*(BigDecimal(rateJS)).*(BigDecimal(rateJszk)).setScale(cs6, RoundingMode.HALF_UP)
          var yhs = BigDecimal(0)
          yhs = 0 //cjje.*(BigDecimal(rateYH)).*(BigDecimal(rateYhzk)).setScale(cs6, RoundingMode.HALF_UP)
          // 征管费的计算
          val zgf = cjje.*(BigDecimal(rateZG)).*(BigDecimal(rateZgzk)).setScale(cs6, RoundingMode.HALF_UP)
          // 风险金的计算
          val fx = cjje.*(BigDecimal(rateFXJ)).*(BigDecimal(rateFxjzk)).setScale(cs6, RoundingMode.HALF_UP)
          // 过户费的计算
          var ghf = BigDecimal(0)
          // 回购收益
          var hgsy = 0
          // 券商过户费的计算
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
          ghf = 0

          sumCjje = sumCjje.+(cjje)
          sumCjsl = sumCjsl.+(cjsl)
          sumJsf = sumJsf.+(jsf)
          sumYhs = sumYhs.+(yhs)
          sumZgf = sumZgf.+(zgf)
          sumGhf = sumGhf.+(ghf)
          sumFxj = sumFxj.+(fx)
          sumGzlx = sumGzlx.+(gzlx)
          sumHgsy = sumHgsy.+(0)
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

        (key, SjsjgFeeSum(DEFAULT_VALUE, sumCjje, sumCjsl, sumYj, sumJsf, sumYhs, sumZgf,
          sumGhf, sumFxj, sumGzlx, sumHgsy,hgts))
    }

    //写入hzjkqs表的最终结果
    val result = feeAdd.flatMap{
      case (key, feeSum) =>
        val fields = key.split(SEPARATE1)
        //发送日期+证券代码+交易席位+股东代码+套账号+证券标志+业务标志+资产代码
        val bcrq = fields(0)
        val zqdm = fields(1)
        val gsdm = fields(2)
        val gddm = fields(3)
        val tzh = fields(4)
        val zqbz = fields(5)
        val ywbz = fields(6)
        val fsetid = fields(7)

        val totalCjje = feeSum.CJJE
        val totalCjsl = feeSum.CJSL
        val fgzlx = feeSum.GZLX
        val fhggain = feeSum.HGSY

        val realYj = feeSum.YJ
        val realJsf = feeSum.JSF
        val realYhs = feeSum.YHS
        val realZgf = feeSum.ZGF
        val realGhf = feeSum.GHF
        val realFxj = feeSum.FXJ

        //买卖标识
        var bs = " "
        if("JJ".equalsIgnoreCase(zqbz) && "LOFRGQR".equalsIgnoreCase(ywbz)){
          bs = "B"
        }else if("ZQ".equalsIgnoreCase(zqbz) && "KZZGP".equalsIgnoreCase(ywbz)){
          bs = "S"
        }else if("GP".equalsIgnoreCase(zqbz) && "KZZGP".equalsIgnoreCase(ywbz)){
          bs = "B"
        }else if("QY".equalsIgnoreCase(zqbz) && "QZ".equalsIgnoreCase(ywbz)){
          bs = "B"
        }else if("KZZSH".equalsIgnoreCase(ywbz) || "QYZQSH".equalsIgnoreCase(ywbz) || "FLKZZSH".equalsIgnoreCase(ywbz)){
          bs = "S"
        }else if("GP".equalsIgnoreCase(zqbz) && "YYSell".equalsIgnoreCase(ywbz)){
          bs = "S"
        }else if("QZ".equalsIgnoreCase(zqbz) && "RZQZXQ".equalsIgnoreCase(ywbz)){
          bs = "S"
        }else if("QZ".equalsIgnoreCase(zqbz) && "RGQZXQ".equalsIgnoreCase(ywbz)){
          bs = "B"
        }else if(("JJFECF".equalsIgnoreCase(zqbz)  || "JJZHYW".equalsIgnoreCase(zqbz)) && totalCjsl>=0 ){
          bs = "B"
        }else if(("JJFECF".equalsIgnoreCase(zqbz)  || "JJZHYW".equalsIgnoreCase(zqbz)) && totalCjsl<0 ){
          bs = "S"
        }else if("JJ".equalsIgnoreCase(zqbz) && "FJJJPDZH".equalsIgnoreCase(ywbz)  && totalCjsl>=0 ){
          bs = "B"
        }else if("JJ".equalsIgnoreCase(zqbz) && "FJJJPDZH".equalsIgnoreCase(ywbz)  && totalCjsl< 0 ){
          bs = "S"
        }else if("PX".equalsIgnoreCase(ywbz)  || "ZQPX".equalsIgnoreCase(ywbz) || "JJPX".equalsIgnoreCase(ywbz) || "XJDJ".equalsIgnoreCase(ywbz) ){
          bs = "S"
        }else if("QY".equalsIgnoreCase(zqbz) && "GFDJ".equalsIgnoreCase(ywbz) ){
          bs = "B"
        }else if("PG".equalsIgnoreCase(ywbz) || "KZZXZ".equalsIgnoreCase(ywbz) ){
          bs = "B"
        }else if("ZTGYW".equalsIgnoreCase(zqbz) && ("LOFZC".equalsIgnoreCase(ywbz) || "ETFZC".equalsIgnoreCase(ywbz) )){
          bs = "S"
        }else if("ZTGYW".equalsIgnoreCase(zqbz) && ("LOFZR".equalsIgnoreCase(ywbz) || "ETFZR".equalsIgnoreCase(ywbz) )){
          bs = "B"
        }else if("QY".equalsIgnoreCase(zqbz) && ("SG".equalsIgnoreCase(ywbz) || "GFDJ".equalsIgnoreCase(ywbz) )){
          bs = "B"
        }

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
        val arrs = new ArrayBuffer[Hzjkqs]()
        val ele = Hzjkqs(
          fsetid,
          bcrq,
          bcrq,
          zqdm,
          SH,
          gsdm,
          bs,
          totalCjje.formatted(DEFAULT_DIGIT_FORMAT),
          totalCjsl.formatted(DEFAULT_DIGIT_FORMAT),
          realYj.formatted(DEFAULT_DIGIT_FORMAT),
          realJsf.formatted(DEFAULT_DIGIT_FORMAT),
          realYhs.formatted(DEFAULT_DIGIT_FORMAT),
          realZgf.formatted(DEFAULT_DIGIT_FORMAT),
          realGhf.formatted(DEFAULT_DIGIT_FORMAT),
          realFxj.formatted(DEFAULT_DIGIT_FORMAT),
          DEFAULT_VALUE_0,  ////其他费用
          fgzlx.formatted(DEFAULT_DIGIT_FORMAT),
          fhggain.formatted(DEFAULT_DIGIT_FORMAT),
          fsfje.formatted(DEFAULT_DIGIT_FORMAT),
          zqbz,
          ywbz,
          DEFAULT_VALUE_SPACE, //交易标志
          "N",  //清算标志
          zqdm,
          "PT",  //交易方式
          "1",  //审核
          DEFAULT_VALUE_SPACE,
          DEFAULT_VALUE_SPACE,
          DEFAULT_VALUE_0,  //指令号
          DEFAULT_VALUE_SPACE,
          DEFAULT_VALUE_0,
          gddm,
          DEFAULT_VALUE_SPACE,
          DEFAULT_VALUE_SPACE,
          DEFAULT_VALUE_SPACE,
          DEFAULT_VALUE_SPACE,
          DEFAULT_VALUE_SPACE,
          DEFAULT_VALUE_SPACE,
          "sjsjg",
          "RMB",
          DEFAULT_VALUE_SPACE,
          DEFAULT_VALUE_SPACE,
          DEFAULT_VALUE_SPACE,
          DEFAULT_VALUE_SPACE,
          DEFAULT_VALUE_SPACE
        )
        arrs.append(ele)
        arrs
    }
    //将结果输出
    import spark.implicits._
    BasicUtils.outputMySql(result.toDF(), "sjsjgPTDataDoMakeResult")

    val outputPath = BasicUtils.getOutputFilePath("/sjsjg"+ywrq.substring(4)+"result")
    BasicUtils.outputHdfs(result.toDF(),outputPath)
  }

}
