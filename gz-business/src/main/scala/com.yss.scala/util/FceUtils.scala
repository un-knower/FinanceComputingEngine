package com.yss.scala.util

import com.yss.scala.core.ShghContants._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

/**
  * @auther: wuson
  * @date: 2018/11/12
  * @version: 1.0.0
  * @desc: 提取相关接口中的公共方法
  */
object FceUtils {

  /**
    * 加载佣金表并转换成 Broadcast[collection.Map[String, String]]结构
    *
    * @param sc   ：SparkContext
    * @param findate ：业务日期  注意（yyyy-MM-dd)
    * @return ：Broadcast[collection.Map[String, String
    *         key = 资产代码+证券类别+市场+交易席位/公司代码
    *         value = 启用日期+利率+折扣+最低佣金值
    */
  def loadYjb(sc: SparkContext, findate: String) = {
    val yjPath = BasicUtils.getDailyInputFilePath(TABLE_NAME_A117CSJYLV)
    val yjb = sc.textFile(yjPath)
    //将佣金表转换成map结构
    val yjbMap = yjb
      .filter(row => {
        val fields = row.split(SEPARATE2)
        val startDate = fields(16) //启用日期
        if (startDate.compareTo(findate) <= 0) true else false
      })
      .map(row => {
        val fields = row.split(SEPARATE2)
        val fsetid = fields(1) //资产代码
        val zqlb = fields(3) //证券类别
        val sh = fields(4) //市场
        val lv = fields(5) //利率
        val minLv = fields(6) //最低利率
        val startDate = fields(16) //启用日期
        //      val zch = row.get(15).toString // 资产
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
        })(0) //获取最近的日期数据
      })
      .collectAsMap()
    sc.broadcast(yjbMap)
  }

  /**
    * 加载费率表并转换成 Broadcast[collection.Map[String, String]]结构
    *
    * @param sc   ：SparkContext
    * @param findate ：业务日期 注意（yyyy-MM-dd)
    * @return ：Broadcast[collection.Map[String, String
    *         key = 证券类别+市场+资产号+利率类别
    *         value = 启用日期+利率+折扣+天数
    */
  def loadflb(sc: SparkContext, findate: String) = {
    //公共的费率表
    val flbPath = BasicUtils.getDailyInputFilePath(TATABLE_NAME_JYLV)
    val flb = sc.textFile(flbPath)

    //将费率表转换成map结构
    val flbMap = flb
      .filter(row => {
        //过滤业务日期之后的数据
        val fields = row.split(SEPARATE2)
        val startDate = fields(13) //启用日期
        if (startDate.compareTo(findate) <= 0) true else false
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
        val value = startDate + SEPARATE1 + lv + SEPARATE1 + zk + SEPARATE1 + fother //启用日期+利率+折扣+天数
        (key, value)
      })
      .groupByKey()
      .mapValues(arrs => {
        //获取最近日期的数据
        arrs.toArray.sortWith((str1, str2) => {
          str1.split(SEPARATE2)(0).compareTo(str2.split(SEPARATE2)(0)) > 0
        })(0)
      })
      .collectAsMap()
  }

  /**
    * 加载公共参数表表并转换成 Broadcast[collection.Map[String, String]]结构
    *
    * @param sc   ：SparkContext
    * @param findate ：业务日期 注意（yyyy-MM-dd)
    * @return ：Broadcast[collection.Map[String, String
    *         key = 公共参数名称
    *         value = 公共参数值
    */
  def loadGgcsb(sc: SparkContext, findate: String) = {

    val csbPath = BasicUtils.getDailyInputFilePath(TABLE_NAME_GGCS)
    val csb = sc.textFile(csbPath)

    //将参数表转换成map结构
    val csbMap = csb
      .filter(row => {
        val fields = row.split(SEPARATE2)
        if (fields(5).compareTo(findate) <= 0) true else false
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

  /**
    * 股东账号表csgdzh Broadcast[collection.Map[String, String]]结构
    *
    * @param sc ：SparkContext
    * @return ：Broadcast[collection.Map[String, String
    *         key = 股东代码
    *         value = 套账号
    */
  def loadCsgdzh(sc: SparkContext) = {
    //读取股东账号表，
    val csgdzhPath = BasicUtils.getDailyInputFilePath(TABLE_NAME_GDZH)
    val csgdzhMap = sc.textFile(csgdzhPath)
      .map(row => {
        val fields = row.split(SEPARATE2)
        (fields(0), fields(5))
      }).collectAsMap()

    sc.broadcast(csgdzhMap)
  }

  /**
    * 加载资产信息表 lsetlist
    *
    * @param sc SparkContext
    * @return Broadcast[collection.Map[String, String
    *         key = 套账号
    *         value = 资产id
    */
  def loadLsetlist(sc: SparkContext) = {
    val lsetlistPath = BasicUtils.getDailyInputFilePath(TABLE_NAME_ZCXX)
    val lsetlistMap = sc.textFile(lsetlistPath)
      .map(row => {
        val fields = row.split(SEPARATE2)
        val fsetid = fields(1) // 资产代码
        val fsetcode = fields(2) //套账号
        (fsetcode, fsetid) //根据套账号获取资产代码
      })
      .collectAsMap()
    sc.broadcast(lsetlistMap)
  }

  /**
    * 获取业务日期后的下一个工作日，
    * 此方法应该在driver中执行，不要用在spark的一些算子中
    *
    * @param sc   ：SparkContext
    * @param findate ：业务日期 注意（yyyy-MM-dd)
    * @param offset ：offset = 0,如果ddate不是节假日，就返回ddate，否则取下一个工作日
    * @param ggt ：是否是港股通
    * @return 下一个工作日
    */
  def getCsholiday(sc: SparkContext, findate: String,offset:Int = 0,ggt:Boolean = false):String = {
    //获取节假日列表
    val csholidayPath = BasicUtils.getDailyInputFilePath(TABLE_NAME_HOLIDAY)
    val csholidayList = sc.textFile(csholidayPath)
      .filter(str => {
        val fields = str.split(SEPARATE2)
        val fdate = fields(0)
        val fbz = fields(1)
        val fsh = fields(3)
        val ffbz = if(ggt) "10" else "0"
        if (ffbz.equals(fbz) && FSH.equals(fsh) && fdate.compareTo(findate) >= 0) true
        else false
      })
      .map(str => {
        str.split(SEPARATE2)(0)
      }).collect()
    if(csholidayList.length == 0) return findate

    //递归获取非节假日
    def addDay( findate:String):String = {
      var finalDate:String = findate
      if(!csholidayList.contains(finalDate)){
        return finalDate
      }
      finalDate =  DateUtils.addDays(finalDate,1,DateUtils.YYYY_MM_DD)
      addDay(finalDate)
    }
    // offset = 0,如果ddate不是节假日，就返回ddate，否则直接从下一个工作日开始
    var date = findate
    if(offset != 0 ){
      date =  DateUtils.addDays(findate,1,DateUtils.YYYY_MM_DD)
    }
    addDay(date)
  }

  /**
    *  根据gddm获取tzh
    * @param broadcast_csgdzh 股东账号表数据
    * @param gddm：股东代码
    * @return 资产id
    */
  def getTzh(broadcast_csgdzh: Broadcast[collection.Map[String, String]], gddm: String): String = {
    broadcast_csgdzh.value.getOrElse(gddm, DEFAULT_VALUE)
  }

  /**
    *  根据套账号获取资产id
    * @param broadcast_lsetlist 资产信息表数据
    * @param fsetcode：套账号
    * @return 资产id
    */
  def getFsetId(broadcast_lsetlist: Broadcast[collection.Map[String, String]], fsetcode: String) = {
    broadcast_lsetlist.value.getOrElse(fsetcode, DEFAULT_VALUE)
  }

  /**
    *  根据gddm获取资产id
    * @param broadcast_csgdzh 股东账号表数据
    * @param broadcast_lsetlist 资产信息表数据
    * @param gddm：股东代码
    * @return 资产id
    */
  def getFsetId(broadcast_csgdzh: Broadcast[collection.Map[String, String]],
                broadcast_lsetlist: Broadcast[collection.Map[String, String]], gddm: String) = {
    val fsetcode = broadcast_csgdzh.value.getOrElse(gddm, DEFAULT_VALUE)
    broadcast_lsetlist.value.getOrElse(fsetcode, DEFAULT_VALUE)
  }

  /**
    * 获取佣金利率
    *
    * @param broadcast_yjb 佣金表的源数据信息，Broadcast[collection.Map[String, String
    * @param fsetid        资产id
    * @param ywbz          业务标志
    * @param zqbz          证券标志
    * @param fsh           市场[S,H]
    * @param gsdm          公司代码
    * @param gddm          股东代码
    * @return （佣金利率，佣金折扣，最小佣金值） 默认值（"0"，"0"，"0"）
    */
  def getYjf(broadcast_yjb: Broadcast[collection.Map[String, String]], fsetid: String, ywbz: String,
             zqbz: String, fsh: String, gsdm: String, gddm: String) = {

    val yjMap = broadcast_yjb.value
    var rateYJStr = DEFORT_VALUE3
    var maybeRateYJStr = yjMap.get(fsetid + SEPARATE1 + ywbz + SEPARATE1 + fsh + SEPARATE1 + gsdm)
    if (maybeRateYJStr.isEmpty) {
      maybeRateYJStr = yjMap.get(fsetid + SEPARATE1 + ywbz + SEPARATE1 + fsh + SEPARATE1 + gddm)
      if (maybeRateYJStr.isEmpty) {
        maybeRateYJStr = yjMap.get(fsetid + SEPARATE1 + zqbz + SEPARATE1 + fsh + SEPARATE1 + gsdm)
        if (maybeRateYJStr.isEmpty) {
          maybeRateYJStr = yjMap.get(fsetid + SEPARATE1 + zqbz + SEPARATE1 + fsh + SEPARATE1 + gddm)
        }
      }
    }

    if (maybeRateYJStr.isDefined) rateYJStr = maybeRateYJStr.get
    val rateYJ = rateYJStr.split(SEPARATE1)(1)
    val rateYjzk = rateYJStr.split(SEPARATE1)(2)
    val minYj = rateYJStr.split(SEPARATE1)(3)
    (rateYJ, rateYjzk, minYj)
  }

  /**
    * 获取公共费率 （过户费，征管费，因花费，经手费，券商过户费等）
    *
    * @param broadcast_flb ：公共费率表数据，Broadcast[collection.Map[String, String
    * @param fllb          费率类型：JSF,ZGF,YHF,等
    * @param zqbz          证券类型
    * @param ywbz          业务类型
    * @param sh            市场（S,H)
    * @param zyzch         专用资产号（套账号）
    * @param gyzch         公用资产号（“0”）
    * @return （费率，折扣，天数） 默认值（"0"，"0"，"0"）
    */
  def getCommonFee(broadcast_flb: Broadcast[collection.Map[String, String]], fllb: String, zqbz: String,
                   ywbz: String, sh: String, zyzch: String, gyzch: String) = {

    val flbMap = broadcast_flb.value
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
    (rate, rateZk, hgts)
  }

  /**
    *
    * @param broadcast_ggcsb ：公共参数表数据Broadcast[collection.Map[String, String
    * @param key
    * @return value 默认"0"
    */
  def getGgcs(broadcast_ggcsb: Broadcast[collection.Map[String, String]], key: String) = {
    broadcast_ggcsb.value.getOrElse(key, NO)
  }

}
