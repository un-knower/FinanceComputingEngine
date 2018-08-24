package com.yss.scala.guzhi

import com.yss.scala.dto.{SHGHFee, ShangHaiGuoHu}
import com.yss.scala.util.Util
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.sql.SparkSession

import scala.math.BigDecimal.RoundingMode
import com.yss.scala.guzhi.SHGHContants._

/**
  * @author ws
  * @version 2018-08-08
  *          描述：上海过户动态获取参数
  *          源文件：gdh.dbf
  *          结果表：SHDZGH
  */
object SHGHPlus {

  def main(args: Array[String]): Unit = {
    doIt()
  }


  private def doIt(): Unit = {

    import com.yss.scala.dbf._

    val spark = SparkSession.builder().appName("SHDZGH").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    //原始数据
    val df = spark.sqlContext.dbfFile("C:\\Users\\wuson\\Desktop\\new\\data\\gh23341.dbf")
    //    val df = spark.sqlContext.dbfFile(Util.getInputFilePath("dgh00001.dbf"))

    //参数表,编码格式的转换
    val csb = sc.hadoopFile("C:\\Users\\wuson\\Desktop\\new\\data\\参数.csv", classOf[TextInputFormat], classOf[LongWritable], classOf[Text])
      .map(pair => new String(pair._2.getBytes, 0, pair._2.getLength, "GBK"))

    //费率表
    val flb = Util.readCSV("C:\\Users\\wuson\\Desktop\\new\\data\\交易利率.csv", spark)
    //佣金表
    val yjb = Util.readCSV("C:\\Users\\wuson\\Desktop\\new\\data\\佣金利率.csv", spark)

    //将费率表转换成map结构
    val flbMap = flb.rdd.map(row => {
      val zqlb = row.get(0).toString
      //证券类别
      val sh = row.get(1).toString
      //市场
      val lvlb = row.get(2).toString
      //利率类别
      val lv = row.get(3).toString //利率
      val zk = row.get(5).toString //折扣
      val zch = row.get(10).toString //资产号
      val startDate = row.get(13).toString
      //启用日期
      val key = zqlb + SEPARATE1 + sh + SEPARATE1 + zch + SEPARATE1 + lvlb //证券类别+市场+资产号+利率类别
      val value = startDate + SEPARATE1 + lv + SEPARATE1 + zk //启用日期+利率+折扣
      (key, value)
    }).groupByKey().mapValues(item => { //分组完成后进行排序取最大的启用日期的数据
      item.toArray.sortWith((str1, str2) => {
        str1.split(SEPARATE1)(0) > str2.split(SEPARATE1)(0)
      })(0)
    }).collectAsMap()

    //将参数表转换成map结构
    val csbMap = csb.map(row => {
      val fields = row.replaceAll(""""""", "").split(SEPARATE2)
      val key = fields(0)
      //参数名
      val value = fields(1) //参数值
      (key, value)
    }).collectAsMap()

    /**
      * 获取公共的费率
      * key = 证券类别+市场+资产号+利率类别
      * value = 启用日期+利率+折扣
      * 获取费率时默认的资产为117如果没有则资产改为0，还没有则费率就取0
      */
    var rateJSstr = flbMap.getOrElse(ZCLB + SEPARATE1 + SH + SEPARATE1 + ZYZCH + SEPARATE1 + JSF, DEFORT_VALUE1)
    if (DEFORT_VALUE1.equals(rateJSstr)) rateJSstr = flbMap.getOrElse(ZCLB + SEPARATE1 + SH + SEPARATE1 + GYZCH + SEPARATE1 + JSF, DEFORT_VALUE2)
    val rateJS = rateJSstr.split(SEPARATE1)(1)
    val rateJszk = rateJSstr.split(SEPARATE1)(2)

    var rateYHStr = flbMap.getOrElse(ZCLB + SEPARATE1 + SH + SEPARATE1 + ZYZCH + SEPARATE1 + YHS, DEFORT_VALUE1)
    if (DEFORT_VALUE1.equals(rateYHStr)) rateYHStr = flbMap.getOrElse(ZCLB + SEPARATE1 + SH + SEPARATE1 + GYZCH + SEPARATE1 + YHS, DEFORT_VALUE2)
    val rateYH = rateYHStr.split(SEPARATE1)(1)
    val rateYhzk = rateYHStr.split(SEPARATE1)(2)

    var rateZGStr = flbMap.getOrElse(ZCLB + SEPARATE1 + SH + SEPARATE1 + ZYZCH + SEPARATE1 + ZGF, DEFORT_VALUE1)
    if (DEFORT_VALUE1.equals(rateZGStr)) rateZGStr = flbMap.getOrElse(ZCLB + SEPARATE1 + SH + SEPARATE1 + GYZCH + SEPARATE1 + ZGF, DEFORT_VALUE2)
    val rateZG = rateZGStr.split(SEPARATE1)(1)
    val rateZgzk = rateZGStr.split(SEPARATE1)(2)

    var rateGHStr = flbMap.getOrElse(ZCLB + SEPARATE1 + SH + SEPARATE1 + ZYZCH + SEPARATE1 + GHF, DEFORT_VALUE1)
    if (DEFORT_VALUE1.equals(rateGHStr)) rateGHStr = flbMap.getOrElse(ZCLB + SEPARATE1 + SH + SEPARATE1 + GYZCH + SEPARATE1 + GHF, DEFORT_VALUE2)
    val rateGH = rateGHStr.split(SEPARATE1)(1)
    val rateGhzk = rateGHStr.split(SEPARATE1)(2)

    var rateFXJStr = flbMap.getOrElse(ZCLB + SEPARATE1 + SH + SEPARATE1 + ZYZCH + SEPARATE1 + FXJ, DEFORT_VALUE1)
    if (DEFORT_VALUE1.equals(rateFXJStr)) rateFXJStr = flbMap.getOrElse(ZCLB + SEPARATE1 + SH + SEPARATE1 + GYZCH + SEPARATE1 + FXJ, DEFORT_VALUE2)
    val rateFXJ = rateFXJStr.split(SEPARATE1)(1)
    val rateFxjzk = rateFXJStr.split(SEPARATE1)(2)

    //获取是否的参数
    val cs1 = csbMap(CS1_KEY)
    val cs3 = csbMap(CS3_KEY)
    val cs4 = csbMap(CS4_KEY)
    val cs6 = csbMap(CS6_KEY)

    //获取计算参数
    val con1 = csbMap(CON1_KEY)
    val con2 = csbMap(CON2_KEY)
    val con3 = csbMap(CON3_KEY)
    val con4 = csbMap(CON4_KEY)
    val con5 = csbMap(CON5_KEY)
    val con7 = csbMap(CON7_KEY)
    val con8 = csbMap(CON8_KEY)

    val con11 = csbMap(CON11_KEY)
    val con12 = csbMap(CON12_KEY)
    val con13 = csbMap(CON13_KEY)
    val con14 = csbMap(CON14_KEY)
    val con15 = csbMap(CON15_KEY)
    val con17 = csbMap(CON17_KEY)

    //将佣金表转换成map结构
    val yjbMap = yjb.rdd.map(row => {
      val zqlb = row.get(1).toString //证券类别
      val sh = row.get(2).toString //市场
      val lv = row.get(3).toString //利率
      val minLv = row.get(4).toString //最低利率
      val startDate = row.get(14).toString //启用日期
      //      val zch = row.get(15).toString // 资产
      val zk = row.get(10).toString //折扣
      val fstr1 = row.get(6).toString //交易席位/公司代码
      val key = zqlb + SEPARATE1 + sh + SEPARATE1 + fstr1 //证券类别+市场+交易席位/公司代码
      val value = startDate + SEPARATE1 + lv + SEPARATE1 + zk + SEPARATE1 + minLv //启用日期+利率+折扣+最低佣金值
      (key, value)
    }).groupByKey().mapValues(item => { //分组完成后进行排序取最大的启用日期的数据
      item.toArray.sortWith((str1, str2) => {
        str1.split(SEPARATE1)(0) > str2.split(SEPARATE1)(0)
      })(0)
    }).collectAsMap()

    //将参数表，佣金表进行广播
    val csbValues = sc.broadcast(csbMap)
    val yjbValues = sc.broadcast(yjbMap)

    import spark.implicits._
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


    /** 获取佣金的费率
      * val gsdm = fields(2) //交易席位
      * val bcrq = fields(0) //本次日期
      */
    def getRate(gsdm: String, bcrq: String) = {
      /**
        * 获取佣金费率
        * key=证券类别+市场+交易席位/公司代码
        * value=启用日期+利率+折扣+最低佣金值
        */
      // var rateYJStr = yjbValues.value.getOrElse(ZCLB + SEPARATE1 + SH + SEPARATE1 + gddm, DEFORT_VALUE1)
      // if (DEFORT_VALUE1.eq(rateYJStr))
      val rateYJStr = yjbValues.value.getOrElse(ZCLB + SEPARATE1 + SH + SEPARATE1 + gsdm, DEFORT_VALUE3)
      val rateYJ = rateYJStr.split(SEPARATE1)(1)
      val rateYjzk = rateYJStr.split(SEPARATE1)(2)
      val minYj = rateYJStr.split(SEPARATE1)(3)

      var cs2 = csbValues.value.getOrElse(CS2_KEY, NO)
      if (!(NO.equals(cs2) || (YES).equals(cs2))) {
        //如果时日期格式的话要比较日期 TODO日期格式的比较
        if (bcrq.compareTo(cs2) > 0) {
          cs2 = YES
        } else {
          cs2 = NO
        }
      }
      (rateYJ, rateYjzk, minYj, cs2)
    }

    //第一种  每一笔交易单独计算，最后相加
    val fee1 = value1.map {
      case (key, values) => {
        val fields = key.split(SEPARATE1)
        val bs = fields(3) //买卖方向
        val gsdm = fields(2) //交易席位
        val bcrq = fields(0) //本次日期
        val zqdm = fields(1) //证券代码

        val getRateResult = getRate(gsdm, bcrq)
        val rateYJ: String = getRateResult._1
        val rateYjzk: String = getRateResult._2
        val minYj: String = getRateResult._3
        var cs2: String = getRateResult._4

        val otherFee = BigDecimal(0)
        var sumCjje = BigDecimal(0) //总金额
        var sumCjsl = BigDecimal(0) //总数量
        var sumYj = BigDecimal(0) //总的佣金
        var sumJsf = BigDecimal(0) //总的经手费
        var sumYhs = BigDecimal(0) //总的印花税
        var sumZgf = BigDecimal(0) //总的征管费
        var sumGhf = BigDecimal(0) //总的过户费
        var sumFxj = BigDecimal(0) //总的风险金

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

        if (YES.equals(cs6)) {
          sumYj = sumYj.-(otherFee)
        }
        if (sumYj < BigDecimal(minYj)) {
          sumYj = BigDecimal(minYj)
        }

        (key, SHGHFee("1", sumCjje, sumCjsl, sumYj, sumJsf, sumYhs, sumZgf,
          sumGhf, sumFxj))
      }
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

        val getRateResult = getRate(gsdm, bcrq)
        val rateYJ: String = getRateResult._1
        val rateYjzk: String = getRateResult._2
        val minYj: String = getRateResult._3
        var cs2: String = getRateResult._4

        var sumCjje = BigDecimal(0) //同一个申请编号总金额
        var sumCjsl = BigDecimal(0) //同一个申请编号总数量


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

        if (YES.equals(cs6)) {
          sumYj2 = sumYj2 - otherFee
        }

        if (sumYj2 < BigDecimal(minYj)) {
          sumYj2 = BigDecimal(minYj)
        }

        (bcrq + SEPARATE1 + zqdm + SEPARATE1 + gsdm + SEPARATE1 + bs,
          SHGHFee("2", sumCjje, sumCjsl, sumYj2, sumJsf2, sumYhs2, sumZgf2,
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
        (key, SHGHFee("2", totalCjje, totalCjsl, totalYj2, totalJsf2, totalYhs2, totalZgf2,
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

        val getRateResult = getRate(gsdm, bcrq)

        val rateYJ: String = getRateResult._1
        val rateYjzk: String = getRateResult._2
        val minYj: String = getRateResult._3
        var cs2: String = getRateResult._4

        val otherFee = BigDecimal(0)
        var sumCjje = BigDecimal(0) //同一个申请编号总金额
        var sumCjsl = BigDecimal(0) //同一个申请编号总数量


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

        if (YES.equals(cs6)) {
          sumYj2 = sumYj2 - otherFee
        }

        if (sumYj2 < BigDecimal(minYj)) {
          sumYj2 = BigDecimal(minYj)
        }

        (key, SHGHFee("3", sumCjje, sumCjsl, sumYj2, sumJsf2, sumYhs2, sumZgf2,
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

        if (YES.equals(con7)) {
          realFxj = fee2.sumFxj
        } else if (YES.equals(con17)) {
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
        ShangHaiGuoHu(bcrq, bcrq, ZqDm, FSzsh, Fjyxwh, FBje.formatted("%.2f"), FSje.formatted("%.2f"), FBsl.formatted("%.2f"), FSsl.formatted("%.2f"), FByj.formatted("%.2f"),
          FSyj.formatted("%.2f"), FBjsf.formatted("%.2f"), FSjsf.formatted("%.2f"), FByhs.formatted("%.2f"), FSyhs.formatted("%.2f"), FBzgf.formatted("%.2f"), FSzgf.formatted("%.2f"), FBghf.formatted("%.2f"), FSghf.formatted("%.2f"), FBgzlx.formatted("%.2f"),
          FSgzlx.formatted("%.2f"), FBFxj.formatted("%.2f"), FSFxj.formatted("%.2f"), FBsfje.formatted("%.2f"), FSssje.formatted("%.2f"), FZqbz, FYwbz, FQsbz, FBQtf.formatted("%.2f"), FSQtf.formatted("%.2f"),
          ZqDm, FJyFS, Fsh, Fzzr, Fchk, fzlh, ftzbz, FBQsghf.formatted("%.2f"), FSQsghf.formatted("%.2f"), FGddm, FHGGAIN.formatted("%.2f"))
    }

    Util.outputMySql(result.toDF(), "SHDZGH5")
    spark.stop()

  }

}
