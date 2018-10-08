package com.yss.scala.guzhi

import com.yss.scala.dto.{Hzjkqs, SJSObj, SZStockExchangeObj, ShghFee}
import com.yss.scala.guzhi.ExecutionContants._
import com.yss.scala.util.Util
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.math.BigDecimal.RoundingMode

/**
  * 问题总结：
  * 原数据的金额太小，就算总金额也特别小，导致风险金等。。都是0.00
  * 佣金根据股东代码+证券标志/业务标志取不到  默认是0吗
  * 三种佣金的计算方式 都是金额汇总后再算吗
  * 资产代码的取值规则
  * 第一种： 不带申请编号的单独计算  第二种： 相同申请编号的汇总后计算   第三种 ： 不带申请编号的汇总后计算
  */

object SZStockExchange extends Serializable {

  def main(args: Array[String]): Unit = {

    getResult()
  }


  def getResult() = {

    //从mysql中读取ETL结果
    val spark = SparkSession.builder().appName("SJSV5").master("local[*]").getOrCreate()

    val df = spark.read.format("jdbc").option("url", "jdbc:mysql://192.168.102.120:3306/JJCWGZ")
      .option("user", "root")
      .option("password", "root1234")
      .option("dbtable", "sjsv5_etl_cy")
      .load()
    //  df.rdd.map(row=> row.getAs[String]("strDate")).foreach(println(_))


    val csb = loadLvarlist(spark.sparkContext)
    doExec(df, csb)
  }


  /** 加载公共参数表lvarlist
    * 返回值: 广播变量 key 参数  value : 0 1 是否开启
    *
    * */
  def loadLvarlist(sc: SparkContext) = {
    //公共的参数表
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
    sc.broadcast(csbMap)
  }


  /** 汇总然后进行计算 */
  def doExec(df: DataFrame, csb: Broadcast[collection.Map[String, String]]) = {

    val spark = SparkSession.builder().appName("SJSV5").master("local[*]").getOrCreate()

    val sc = spark.sparkContext


    /** 加载公共费率表和佣金表
      * 佣金：
      * key: 证券类别+市场+交易席位  value: 启动日期+利率+折扣+最低佣金
      * 费率：
      * key:证券类别+市场+套账号+利率类别  value:启动日期+利率+折扣
      *
      * */
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
        .collectAsMap()

      //交易费用表（佣金的三种模式）
      val csbPath = Util.getDailyInputFilePath("A117CSXWFY")
      val jyfy = sc.textFile(csbPath)
      //同一个席位号只能选一个JSF,ZGF
      val jyfyMap = jyfy.map {
        case (row1) => {
          val row = row1.split(",")
          val gddm1 = row(0) //股东代码
          val zqlb = row(2) //证券类别
          val ffylb = row(3) //费用类别 JSF
          val ffyfs = row(4) //费用方式  券商承担 产品承担 计入成本
          val key = gddm1 + SEPARATE1 + zqlb + SEPARATE1 + ffylb
          val value = ffyfs
          (key, value)
        }
      }.collectAsMap()

      (sc.broadcast(yjbMap), sc.broadcast(flbMap), sc.broadcast(jyfyMap))
    }

    val broadcaseFee = loadFeeTables()
    val yjbValues = broadcaseFee._1 //佣金
    val flbValues = broadcaseFee._2 //费率
    val jyfyValues = broadcaseFee._3 //交易费率
    val csbValues = csb


    /**
      * 原始数据转换1  带有申请编号
      * key = 本次日期+证券代码+公司代码/交易席位+买卖+股东代码+套账号+证券标志+业务标志+申请编号
      */
    val value = df.rdd.map(row => {
      val bcrq = row.getAs[String]("strDate") //本次日期
      val zqdm = row.getAs[String]("SecurityID") //证券代码
      val gsdm = row.getAs[String]("ReportingPBUID") //公司代码/交易席位
      val gddm = row.getAs[String]("AccountID") //股东代码
      val bs = row.getAs[String]("Side") //买卖
      val sqbh = row.getAs[String]("SQBH") //申请编号
      val tzh = row.getAs[String]("SETCODE") //套账号
      val zqbz = row.getAs[String]("FZQBZ") //证券标志
      val ywbz = row.getAs[String]("FYWBZ") //业务标志
      val key = bcrq + SEPARATE1 + zqdm + SEPARATE1 + gsdm + SEPARATE1 + bs + SEPARATE1 +
        gddm + SEPARATE1 + tzh + SEPARATE1 + zqbz + SEPARATE1 + ywbz + SEPARATE1 + sqbh
      (key, row)
    }).groupByKey()

    /**
      * 原始数据转换2 不带申请编号
      * key = 本次日期+证券代码+公司代码/交易席位+买卖+股东代码+套账号+证券标志+业务标志
      */
    val value1 = df.rdd.map(row => {
      val bcrq = row.getAs[String]("strDate") //本次日期
      val zqdm = row.getAs[String]("SecurityID") //证券代码
      val gsdm = row.getAs[String]("ReportingPBUID") //公司代码/交易席位
      val gddm = row.getAs[String]("AccountID") //股东代码
      val bs = row.getAs[String]("Side") //买卖
      val sqbh = row.getAs[String]("SQBH") //申请编号
      val tzh = row.getAs[String]("SETCODE") //套账号
      val zqbz = row.getAs[String]("FZQBZ") //证券标志
      val ywbz = row.getAs[String]("FYWBZ") //业务标志
      val key = bcrq + SEPARATE1 + zqdm + SEPARATE1 + gsdm + SEPARATE1 + bs + SEPARATE1 +
        gddm + SEPARATE1 + tzh + SEPARATE1 + zqbz + SEPARATE1 + ywbz
      (key, row)
    }).groupByKey()

    value1.foreach(println(_))

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
    def getRate(gsdm: String, gddm: String, bcrq: String, ywbz: String, zqbz: String, zyzch: String, gyzch: String) = {
      //为了获取启动日期小于等于处理日期的参数
      val flbMap = flbValues.value.mapValues(items => {
        val arr = items.toArray.filter(str => (bcrq.compareTo(str.split(SEPARATE1)(0)) >= 0)).sortWith((str1, str2) => (str1.split(SEPARATE1)(0).compareTo(str2.split(SEPARATE1)(0)) > 0))
        //TODO arr's size is 0
        if (arr.size == 0) throw new Exception("未找到适合的公共费率")
        arr(0) //获取处理日期大于启动日期的最大的一个
      })
      val yjMap = yjbValues.value.mapValues(items => {
        val arr = items.toArray.filter(str => (bcrq.compareTo(str.split(SEPARATE1)(0)) >= 0)).sortWith((str1, str2) => (str1.split(SEPARATE1)(0).compareTo(str2.split(SEPARATE1)(0)) > 0))
        //TODO arr's size is 0
        if (arr.size == 0) throw new Exception("未找到合适的佣金费率")
        arr(0)
      })

      /** 获取费率
        * 将费率类别带进来，根据 证券标志和业务标志  套账号是0还是117来得到费率
        *
        */
      def getCommonFee(fllb: String) = {
        var rateStr = DEFORT_VALUE2
        var maybeRateStr = flbMap.get(ywbz + SEPARATE1 + SH + SEPARATE1 + zyzch + SEPARATE1 + fllb)
        if (maybeRateStr.isEmpty) {
          maybeRateStr = flbMap.get(zqbz + SEPARATE1 + SH + SEPARATE1 + zyzch + SEPARATE1 + fllb)
          if (maybeRateStr.isEmpty) {
            maybeRateStr = flbMap.get(ywbz + SEPARATE1 + SH + SEPARATE1 + gyzch + SEPARATE1 + fllb)
            if (maybeRateStr.isEmpty) {
              maybeRateStr = flbMap.get(zqbz + SEPARATE1 + SH + SEPARATE1 + gyzch + SEPARATE1 + fllb)
            }
          }
        }
        if (maybeRateStr.isDefined) rateStr = maybeRateStr.get
        val rate = rateStr.split(SEPARATE1)(1) //利率
        val rateZk = rateStr.split(SEPARATE1)(2) //折扣
        (rate, rateZk)
      }

      /**
        * 获取佣金费率
        * key=业务标志/证券标志+市场+交易席位/股东代码
        * value=启用日期+利率+折扣+最低佣金值
        */
      def getYjFee() = {
        var rateYJStr = DEFORT_VALUE3
        var maybeRateYJStr= DEFORT_VALUE3
        maybeRateYJStr = yjMap.getOrElse(ywbz + SEPARATE1 + SH + SEPARATE1 + gsdm,"-1")
        if (maybeRateYJStr.equals("-1")) {
           maybeRateYJStr = yjMap.getOrElse(ywbz + SEPARATE1 + SH + SEPARATE1 + gddm,"-1")
          if (maybeRateYJStr.equals("-1")) {
             maybeRateYJStr = yjMap.getOrElse(zqbz + SEPARATE1 + SH + SEPARATE1 + gsdm,"-1")
            if (maybeRateYJStr.equals("-1")) {
              maybeRateYJStr = yjMap.getOrElse(zqbz + SEPARATE1 + SH + SEPARATE1 + gddm,"-1")
            }
          }
        }
        if (maybeRateYJStr!="-1") rateYJStr = maybeRateYJStr
        val rateYJ = rateYJStr.split(SEPARATE1)(1) //利率
        val rateYjzk = rateYJStr.split(SEPARATE1)(2) //折扣
        val minYj = rateYJStr.split(SEPARATE1)(3) //最低佣金
        (rateYJ, rateYjzk, minYj)
      }

      val rateJS = getCommonFee(JSF)

      var rateYH = getCommonFee(YHS)

      var rateZG = getCommonFee(ZGF)

      var rateGH = getCommonFee(GHF)

      var rateFXJ = getCommonFee(FXJ)

      val yjFee = getYjFee()

      (rateJS._1, rateJS._2, rateYH._1, rateYH._2, rateZG._1, rateZG._2, rateGH._1, rateGH._2, rateFXJ._1, rateFXJ._2, yjFee._1, yjFee._2, yjFee._3)
    }

    /**
      * 根据套账号获取公共参数
      *
      * @param tzh 套账号
      **/
    def getGgcs(tzh: String) = {
      //获取是否的参数
      val cs1 = csbValues.value.getOrElse(tzh + CS1_KEY, "-1") //是否开启佣金包含经手费，证管费
      var cs2 = csbValues.value.getOrElse(tzh + CON18_KEY, "-1") //深圳佣金计算保留位数
      val cs3 = csbValues.value.getOrElse(tzh + CON23_KEY, "-1") //深圳佣金计算费用保留位数
      val cs4 = csbValues.value.getOrElse(tzh + CS4_KEY, "-1") //是否开启计算佣金减去风险金
      val cs5 = csbValues.value.getOrElse(tzh + CS6_KEY, "-1") //是否开启计算佣金减去结算费
      val cs6 = csbValues.value.getOrElse(tzh + CON24_KEY, "-1") //深交所证管费和经手费分别计算


      (cs1, cs2, cs3, cs4, cs5, cs6)
    }

    /**
      * 根据套账号获取计算参数
      *
      * @param tzh 套账号
      * @return
      */
    def getJsgz(tzh: String) = {
      val cs6 = csbValues.value.getOrElse(tzh + CON8_KEY, "-1") //是否开启实际收付金额包含佣金

      //获取计算参数
      val con1 = csbValues.value.getOrElse(tzh + CON1_KEY, "-1") //是否勾选按申请编号汇总计算经手费
      val con2 = csbValues.value.getOrElse(tzh + CON2_KEY, "-1") //是否勾选按申请编号汇总计算征管费
      val con3 = csbValues.value.getOrElse(tzh + CON3_KEY, "-1") //是否勾选按申请编号汇总计算过户费
      val con4 = csbValues.value.getOrElse(tzh + CON4_KEY, "-1") //是否勾选按申请编号汇总计算印花税
      val con5 = csbValues.value.getOrElse(tzh + CON5_KEY, "-1") //是否勾选H按申请编号汇总计算佣金
      val con6 = csbValues.value.getOrElse(tzh + CON7_KEY, "-1") //是否勾选H按申请编号汇总计算风险金

      val con7 = csbValues.value.getOrElse(tzh + CON11_KEY, "-1") //是否开启按成交记录计算经手费
      val con8 = csbValues.value.getOrElse(tzh + CON12_KEY, "-1") //是否开启按成交记录计算征管费
      val con9 = csbValues.value.getOrElse(tzh + CON13_KEY, "-1") //是否开启按成交记录计算过户费
      val con10 = csbValues.value.getOrElse(tzh + CON14_KEY, "-1") //是否开启按成交记录计算印花税
      val con11 = csbValues.value.getOrElse(tzh + CON15_KEY, "-1") //是否开启H按成交记录计算佣金
      val con12 = csbValues.value.getOrElse(tzh + CON17_KEY, "-1") //是否开启H按成交记录计算风险金
      (cs6, con1, con2, con3, con4, con5, con6, con7, con8, con9, con10, con11, con12)
    }

    //第一种   不带申请编号   每一笔交易单独计算，最后相加
    val fee1 = value1.map {
      case (key, values) =>
        val fields = key.split(SEPARATE1)
        val bs = fields(3) //买卖方向
      val gsdm = fields(2) //交易席位
      val bcrq = fields(0) //本次日期
      val zqdm = fields(1) //证券代码
      val gddm = fields(4) //股东代码
      val tzh = fields(5) //套账号
      val zqbz = fields(6) //证券标志
      val ywbz = fields(7) //业务标志

        val getRateResult = getRate(gsdm, gddm, bcrq, ywbz, zqbz, tzh, GYZCH)
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
        // var sumGzlx = BigDecimal(0) //总的国债利息
        // var sumHgsy = BigDecimal(0) //总的回购收益

        val csResults = getGgcs(tzh)
        val cs1 = csResults._1
        var cs2 = csResults._2 //深圳佣金计算费用保留位数
        val cs3 = csResults._3
        val cs4 = csResults._4
        val cs5 = csResults._5
        val cs6 = csResults._6 //深交所证管费和经手费分别计算


        for (row <- values) {
          val cjje1 = BigDecimal(row.getAs[String]("LastPx"))
          val cjsl = BigDecimal(row.getAs[String]("LastQty"))
          val cjje=cjje1.*(cjsl)

          //  val gzlx = BigDecimal(row.getAs[String]("FGZLX"))
          // val hgsy = BigDecimal(row.getAs[String]("FHGGAIN"))

          var yhs = BigDecimal(0)
          var jsf = BigDecimal(0)
          // 买不计算印花税
          if (SALE.equals(bs)) {
            //印花税的计算
            yhs = cjje.*(BigDecimal(rateYH)).*(BigDecimal(rateYhzk)).setScale(2, RoundingMode.HALF_UP)
          }

          //征管费的计算
          var zgf = cjje.*(BigDecimal(rateZG)).*(BigDecimal(rateZgzk)).setScale(2, RoundingMode.HALF_UP)
          //经手费的计算
          if (cs6.equals("-1") || cs6.equals("0")) { // 不启用 经手费 =成交金额*(经手费率*折扣率+征管费*折扣率)
            jsf = cjje.*(BigDecimal(rateJS).*(BigDecimal(rateJszk) )+ BigDecimal(rateZG).*(BigDecimal(rateZgzk))).setScale(2, RoundingMode.HALF_UP)
          } else {
            jsf = cjje.*(BigDecimal(rateJS)).*(BigDecimal(rateJszk)).setScale(2, RoundingMode.HALF_UP)
          }
          //风险金的计算
          var fx = cjje.*(BigDecimal(rateFXJ)).*(BigDecimal(rateFxjzk)).setScale(2, RoundingMode.HALF_UP)

          //过户费的计算
          var ghf = cjje.*(BigDecimal(rateGH)).*(BigDecimal(rateGhzk)).setScale(2, RoundingMode.HALF_UP)

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
          //   sumGzlx = sumGzlx.+(gzlx)
          //   sumHgsy = sumHgsy.+(hgsy)
        }

        //佣金的计算

        if (cs2.equals("-1") || cs2.equals("0")) {
          sumYj = sumCjje.*(BigDecimal(rateYJ)).*(BigDecimal(rateYjzk)).setScale(0,RoundingMode.DOWN)

        } else {
          sumYj = sumCjje.*(BigDecimal(rateYJ)).*(BigDecimal(rateYjzk)).setScale(cs2.toInt, RoundingMode.HALF_UP)
        }

        if (NO.equals(cs1)) { //经手费,证管费
          sumYj = sumYj.-(sumJsf).-(sumZgf)
        } else if (YES.equals(cs1)) {
          var JSF = jyfyValues.value.getOrElse(gddm + SEPARATE1 + zqbz + SEPARATE1 + "JSF", "-1") //如果这个key的话，取得这个值，没有取-1
          var ZGF = jyfyValues.value.getOrElse(gddm + SEPARATE1 + zqbz + SEPARATE1 + "ZGF", "-1")
          var JSF1 = jyfyValues.value.getOrElse(gsdm + SEPARATE1 + zqbz + SEPARATE1 + "JSF", "-1")
          var ZGF1 = jyfyValues.value.getOrElse(gsdm + SEPARATE1 + zqbz + SEPARATE1 + "ZGF", "-1")
          var JSF2 = jyfyValues.value.getOrElse(gsdm + SEPARATE1 + ywbz + SEPARATE1 + "JSF", "-1")
          var ZGF2 = jyfyValues.value.getOrElse(gsdm + SEPARATE1 + ywbz + SEPARATE1 + "ZGF", "-1")

          if (JSF == 0 || JSF1 == 0 || JSF == 0) { //有经手费，有证管费

            if (ZGF == 0 || ZGF1 == 0 || ZGF2 == 0) {
              //有经手费，有证管费

              //佣金-征管费-经手费
              sumYj = sumYj - sumJsf - sumZgf
            } else {
              //有经手费，没证管费
              //佣金-经手费
              sumYj = sumYj - sumJsf
            }
          } else if (ZGF == 0 || ZGF1 == 0 || ZGF2 == 0) { //没有经手费 ，有证管费
            sumYj = sumYj - sumZgf
          }
          sumYj
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
        (key, SJSObj("1", sumCjje, sumCjsl, sumYj, sumJsf, sumYhs, sumZgf,
          sumGhf, sumFxj))
    }

    //第二种 相同申请编号的金额汇总*费率，各申请编号汇总后的金额相加
    val fee2 = value.map {
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
        val otherFee = BigDecimal(0)

        val getRateResult = getRate(gsdm, gddm, bcrq, ywbz, zqbz, tzh, GYZCH)
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

        val csResults = getGgcs(tzh)
        val cs1 = csResults._1
        var cs2 = csResults._2
        val cs3 = csResults._3
        val cs4 = csResults._4
        val cs5 = csResults._5
        val cs6 = csResults._6


        for (row <- values) {
          val cjje = BigDecimal(row.getAs[String]("LastPx"))
          val cjsl = BigDecimal(row.getAs[String]("LastQty"))
          sumCjje = sumCjje.+((cjje).*(cjsl))
          sumCjsl = sumCjsl.+(cjsl)
        }
        var sumJsf2 = BigDecimal(0)

        //同一个申请编号总的征管费
        var sumZgf2 = sumCjje.*(BigDecimal(rateZG)).*(BigDecimal(rateZgzk)).setScale(2, RoundingMode.HALF_UP)

        //同一个申请编号总的经手费
        if (cs6.equals("-1") || cs6.equals("0")) { // 不启用 经手费 =成交金额*(经手费率*折扣率+征管费*折扣率)
          sumJsf2 = sumCjje.*(BigDecimal(rateJS).*(BigDecimal(rateJszk) )+BigDecimal(rateZG).*(BigDecimal(rateZgzk))).setScale(2, RoundingMode.HALF_UP)
        } else {
          sumJsf2 = sumCjje.*(BigDecimal(rateJS)).*(BigDecimal(rateJszk)).setScale(2, RoundingMode.HALF_UP)
        }
        //同一个申请编号总的印花税
        var sumYhs2 = BigDecimal(0)
        if (SALE.equals(bs)) {
          sumYhs2 = sumCjje.*(BigDecimal(rateYH)).*(BigDecimal(rateYhzk)).setScale(2, RoundingMode.HALF_UP)
        }
        //同一个申请编号总的风险金
        var sumFxj2 = sumCjje.*(BigDecimal(rateFXJ)).*(BigDecimal(rateFxjzk)).setScale(2, RoundingMode.HALF_UP)


        //同一个申请编号总的过户费
        var sumGhf2 = sumCjje.*(BigDecimal(rateGH)).*(BigDecimal(rateGhzk)).setScale(2, RoundingMode.HALF_UP)


        var sumYj2 = BigDecimal(0)

        //同一个申请编号总的佣金 （按申请编号汇总）
        if (cs2.equals("-1") || cs2.equals("0")) {
          var sumYj2 = sumCjje.*(BigDecimal(rateYJ)).*(BigDecimal(rateYjzk)).setScale(0,RoundingMode.DOWN)

        } else {
          var sumYj2 = sumCjje.*(BigDecimal(rateYJ)).*(BigDecimal(rateYjzk)).setScale(cs2.toInt, RoundingMode.HALF_UP)
        }

        if (NO.equals(cs1)) {
          sumYj2 = sumYj2 - sumJsf2 - sumZgf2

        } else if (YES.equals(cs1)) {
          var JSF = jyfyValues.value.getOrElse(gddm + SEPARATE1 + zqbz + SEPARATE1 + "JSF", "-1") //如果这个key的话，取得这个值，没有取-1
          var ZGF = jyfyValues.value.getOrElse(gddm + SEPARATE1 + zqbz + SEPARATE1 + "ZGF", "-1")
          var JSF1 = jyfyValues.value.getOrElse(gsdm + SEPARATE1 + zqbz + SEPARATE1 + "JSF", "-1")
          var ZGF1 = jyfyValues.value.getOrElse(gsdm + SEPARATE1 + zqbz + SEPARATE1 + "ZGF", "-1")
          var JSF2 = jyfyValues.value.getOrElse(gsdm + SEPARATE1 + ywbz + SEPARATE1 + "JSF", "-1")
          var ZGF2 = jyfyValues.value.getOrElse(gsdm + SEPARATE1 + ywbz + SEPARATE1 + "ZGF", "-1")

          if (JSF == 0 || JSF1 == 0 || JSF == 0) { //有经手费，有证管费

            if (ZGF == 0 || ZGF1 == 0 || ZGF2 == 0) {
              //有经手费，有证管费

              //佣金-征管费-经手费
              sumYj2 = sumYj2 - sumJsf2 - sumZgf2
            } else {
              //有经手费，没证管费
              //佣金-经手费
              sumYj2 = sumYj2 - sumJsf2
            }
          } else if (ZGF == 0 || ZGF1 == 0 || ZGF2 == 0) { //没有经手费 ，有证管费
            sumYj2 = sumYj2 - sumZgf2
          }
          sumYj2
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

        (bcrq + SEPARATE1 + zqdm + SEPARATE1 + gsdm + SEPARATE1 + bs + SEPARATE1 +
          gddm + SEPARATE1 + tzh + SEPARATE1 + zqbz + SEPARATE1 + ywbz,
          ShghFee("2", sumCjje, sumCjsl, sumYj2, sumJsf2, sumYhs2, sumZgf2,
            sumGhf2, sumFxj2, BigDecimal(0), BigDecimal(0)))
    }
      .groupByKey()
      .map {
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
            totalGhf2, totalFxj2, BigDecimal(0), BigDecimal(0)))
      }


    //第三种 金额汇总*费率  key不加申请编号
    val fee3 = value1.map {
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

        val getRateResult = getRate(gsdm, gddm, bcrq, ywbz, zqbz, tzh, GYZCH)
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

        val csResults = getGgcs(tzh)
        val cs1 = csResults._1
        var cs2 = csResults._2
        val cs3 = csResults._3
        val cs4 = csResults._4
        val cs5 = csResults._5
        val cs6 = csResults._6


        for (row <- values) {
          val cjje = BigDecimal(row.getAs[String]("LastPx"))
          val cjsl = BigDecimal(row.getAs[String]("LastQty"))
          sumCjje = sumCjje.+((cjje).*(cjsl))
          sumCjsl = sumCjsl.+(cjsl)
        }
        var sumJsf2 = BigDecimal(0)

        //总印花税
        var sumYhs2 = BigDecimal(0)
        if (SALE.equals(bs)) {
          sumYhs2 = sumCjje.*(BigDecimal(rateYH)).*(BigDecimal(rateYhzk)).setScale(2, RoundingMode.HALF_UP)
        }
        //总证管费
        val sumZgf2 = sumCjje.*(BigDecimal(rateZG)).*(BigDecimal(rateZgzk)).setScale(2, RoundingMode.HALF_UP)
        //经手费
        if (cs6.equals("-1") || cs6.equals("0")) { // 不启用 经手费 =成交金额*(经手费率*折扣率+征管费*折扣率)
          sumJsf2 = sumCjje.*((BigDecimal(rateJS)).*(BigDecimal(rateJszk) )+BigDecimal(rateZG).*(BigDecimal(rateZgzk))).setScale(2, RoundingMode.HALF_UP)
        } else {
          sumJsf2 = sumCjje.*(BigDecimal(rateJS)).*(BigDecimal(rateJszk)).setScale(2, RoundingMode.HALF_UP)
        }

        //总风险金
        val sumFxj2 = sumCjje.*(BigDecimal(rateFXJ)).*(BigDecimal(rateFxjzk)).setScale(2, RoundingMode.HALF_UP)
      //同一个申请编号总的过户费
      var sumGhf2 = sumCjje.*(BigDecimal(rateGH)).*(BigDecimal(rateGhzk)).setScale(2, RoundingMode.HALF_UP)

        var sumYj2 = BigDecimal(0)
        if (cs2.equals("-1") || cs2.equals("0")) {
          var sumYj2 = sumCjje.*(BigDecimal(rateYJ)).*(BigDecimal(rateYjzk)).setScale(0,RoundingMode.DOWN)

        } else {
          var sumYj2 = sumCjje.*(BigDecimal(rateYJ)).*(BigDecimal(rateYjzk)).setScale(cs2.toInt, RoundingMode.HALF_UP)
        }

        if (NO.equals(cs1)) {
          sumYj2 = sumYj2 - sumJsf2 - sumZgf2
        } else if (YES.equals(cs1)) {
          var JSF = jyfyValues.value.getOrElse(gddm + SEPARATE1 + zqbz + SEPARATE1 + "JSF", "-1") //如果这个key的话，取得这个值，没有取-1
          var ZGF = jyfyValues.value.getOrElse(gddm + SEPARATE1 + zqbz + SEPARATE1 + "ZGF", "-1")
          var JSF1 = jyfyValues.value.getOrElse(gsdm + SEPARATE1 + zqbz + SEPARATE1 + "JSF", "-1")
          var ZGF1 = jyfyValues.value.getOrElse(gsdm + SEPARATE1 + zqbz + SEPARATE1 + "ZGF", "-1")
          var JSF2 = jyfyValues.value.getOrElse(gsdm + SEPARATE1 + ywbz + SEPARATE1 + "JSF", "-1")
          var ZGF2 = jyfyValues.value.getOrElse(gsdm + SEPARATE1 + ywbz + SEPARATE1 + "ZGF", "-1")

          if (JSF == 0 || JSF1 == 0 || JSF == 0) { //有经手费，有证管费

            if (ZGF == 0 || ZGF1 == 0 || ZGF2 == 0) {
              //有经手费，有证管费

              //佣金-征管费-经手费
              sumYj2 = sumYj2 - sumJsf2 - sumZgf2
            } else {
              //有经手费，没证管费
              //佣金-经手费
              sumYj2 = sumYj2 - sumJsf2
            }
          } else if (ZGF == 0 || ZGF1 == 0 || ZGF2 == 0) { //没有经手费 ，有证管费
            sumYj2 = sumYj2 - sumZgf2
          }
          sumYj2
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
          sumGhf2, sumFxj2, BigDecimal(0), BigDecimal(0)))
    }

    //将三种结果串联起来
    val middle = fee1.join(fee2).join(fee3)

    //最终结果
    val result = middle.map {
      case (key, ((fee1, fee2), fee3)) =>
        val fields = key.split(SEPARATE1)
        val bs = fields(3) //买卖方向
      val gsdm = fields(2) //交易席位
      val bcrq = fields(0) //本次日期
      val zqdm = fields(1) //证券代码
      val gddm = fields(4)
        val tzh = fields(5)
        val zqbz = fields(6)
        val ywbz = fields(7)

        val totalCjje = fee1.sumCjje
        val totalCjsl = fee1.sumCjsl
        //  val fgzlx = fee1.sumGzlx
        //  val fhggain = fee1.sumHgsy

        var realYj = BigDecimal(0)
        var realJsf = BigDecimal(0)
        var realYhs = BigDecimal(0)
        var realZgf = BigDecimal(0)
        var realGhf = BigDecimal(0)
        var realFxj = BigDecimal(0)

        val jsResult = getJsgz(tzh)
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
          realZgf = fee1.sumZgf
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

        var fsfje = totalCjje.+(realJsf).+(realZgf).+(realGhf)
        //        var FSssje = FSje.-(FSjsf).-(FSzgf).-(FSghf).-(FSyhs)
        if (YES.equals(con8)) {
          fsfje += realYj
          //          FSssje -= FByj
        }
        SZStockExchangeObj(bcrq,
          bcrq, zqdm, SH, gsdm, bs,
          totalCjje.formatted("%.2f"),
          totalCjsl.formatted("%.2f"),
          realYj.formatted("%.2f"),
          realJsf.formatted("%.2f"),
          realYhs.formatted("%.2f"),
          realZgf.formatted("%.2f"),
          realGhf.formatted("%.2f"),
          realFxj.formatted("%.2f"),
          "0",
          //  fhggain.formatted("%.2f"),
          fsfje.formatted("%.2f"),
          zqbz, ywbz,
          "N", "0", zqdm, "PT", "1", "", "", "0", "", "0",
          gddm, "", "", "", "", "", "", "", "", "", "", "", "", ""
        )
    }
    //将结果输出
    import spark.implicits._
      Util.outputMySql(result.toDF(), "sz_stock")
       result.toDF.show(40)
  }


}
