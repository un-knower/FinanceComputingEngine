package com.yss.test.scala

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.math.BigDecimal.RoundingMode

case class DGH000013(FDATE: String,
                     FINDATE: String,
                     FZQDM: String,
                     FSZSH: String,
                     FJYXWH: String,
                     FBJE: String,
                     FSJE: String,
                     FBSL: String,
                     FSSL: String,
                     FBYJ: String,
                     FSYJ: String,
                     FBJSF: String,
                     FSJSF: String,
                     FBYHS: String,
                     FSYHS: String,
                     FBZGF: String,
                     FSZGF: String,
                     FBGHF: String,
                     FSGHF: String,
                     FBGZLX: String,
                     FSGZLX: String,
                     FHGGAIN: String,
                     FBFXJ: String,
                     FSFXJ: String,
                     FBSFJE: String,
                     FSSSJE: String,
                     FZQBZ: String,
                     FYWBZ: String,
                     FQSBZ: String,
                     FBQTF: String,
                     FSQTF: String,
                     ZQDM: String,
                     FJYFS: String,
                     FSH: String,
                     FZZR: String,
                     FCHK: String,
                     FZLH: String,
                     FTZBZ: String,
                     FBQSGHF: String,
                     FSQSGHF: String,
                     FGDDM: String)

/**
  * var sumCjje = BigDecimal(0) //同一个申请编号总金额
  * var sumCjsl = BigDecimal(0) //同一个申请编号总数量
  * var sumYj = BigDecimal(0) //同一个申请编号总的佣金 （按成交记录）
  * var sumJsf = BigDecimal(0) //同一个申请编号总的经手费
  * var sumYhs = BigDecimal(0) //同一个申请编号总的印花税
  * var sumZgf = BigDecimal(0) //同一个申请编号总的征管费
  * var sumGhf = BigDecimal(0) //同一个申请编号总的过户费
  * var sumFxj = BigDecimal(0) //同一个申请编号总的风险金
  *
  */
case class Fee(ctype:String,sumCjje: BigDecimal, sumCjsl: BigDecimal, sumYj: BigDecimal, sumJsf: BigDecimal, sumYhs: BigDecimal, sumZgf: BigDecimal,
               sumGhf: BigDecimal, sumFxj: BigDecimal)

/**
  * @author ws
  * @version 2018-08-08
  *          描述：上海大宗过户
  *          源文件：gdh.dbf
  *          结果表：HZJKQS
  */
object SHDZGH3 {

  def main(args: Array[String]): Unit = {
    doIt()
//    test01()
  }

  def test01() = {
//    val arr1 = Array("a", "b", "c")
//    println(arr1(0))
//    val str = arr1.sortWith((st1, str2) => st1 > str2)(0)
//    println(str)
    val map1 = Map("中国"->1,"张三"->2)
    println(map1("中国"))
  }

  private def doIt(): Unit = {


    val spark = SparkSession.builder().appName("SHDZGH").master("local[*]").getOrCreate()
    //    val df = spark.sqlContext.dbfFile(Util.getInputFilePath("dgh00001.dbf"))
    val sc = spark.sparkContext
    //原始数据
//    val df = spark.sqlContext.dbfFile("C:\\Users\\wuson\\Desktop\\new\\data\\dgh2250120180418.dbf")
    //参数表
//    val csb = spark.read.format("csv")
//      .option("sep", ",")
//      .option("inferSchema", "false")
//      .option("header", "true")
//      .load("C:\\Users\\wuson\\Desktop\\new\\data\\参数.csv")
    val csb = sc.hadoopFile("C:\\Users\\yupan\\Desktop\\参数.csv", classOf[TextInputFormat], classOf[LongWritable], classOf[Text])
          .map(pair => new String(pair._2.getBytes, 0, pair._2.getLength,"GBK"))

    csb.foreach(println)
    println

    //费率表
//    val flb = spark.read.format("csv")
//      .option("sep", ",")
//      .option("inferSchema", "false")
//      .option("header", "true")
//      .load("C:\\Users\\wuson\\Desktop\\new\\data\\交易利率.csv")
    //佣金表
//    val yjb = spark.read.format("csv")
//      .option("sep", ",")
//      .option("inferSchema", "false")
//      .option("header", "true")
//      .load("C:\\Users\\wuson\\Desktop\\new\\data\\佣金利率.csv")

    //将参数表转换成map结构
    val csbMap = csb.map(row => {
      val fields = row.split(",")
      val key = fields(0) //参数名
      val value = fields(1) //参数值
      (key, value)
    }).collectAsMap()

    //将费率表转换成map结构
//    val flbMap = flb.rdd.map(row => {
//      val zqlb = row.get(0).toString
//      //证券类别
//      val sh = row.get(1).toString
//      //市场
//      val lvlb = row.get(2).toString
//      //利率类别
//      val lv = row.get(3).toString //利率
//      val zk = row.get(5).toString //折扣
//      val zch = row.get(10).toString //资产号
//      val startDate = row.get(13).toString
//      //启用日期
//      val key = zqlb + "-" + sh + "-" + zch + "-" + lvlb //证券类别+市场+资产号+利率类别
//      val value = startDate + "-" + lv + "-" + zk //启用日期+利率+折扣
//      (key, value)
//    }).groupByKey().mapValues(item => { //分组完成后进行排序取最大的启用日期的数据
//      item.toArray.sortWith((str1, str2) => {
//        str1.split("-")(0) > str2.split("-")(0)
//      })(0)
//    }).collectAsMap()
//
//    //将佣金表转换成map结构
//    val yjbMap = yjb.rdd.map(row => {
//      val zqlb = row.get(1).toString //证券类别
//      val sh = row.get(2).toString //市场
//      val lv = row.get(3).toString //利率
//      val minLv = row.get(4).toString //最低利率
//      val startDate = row.get(14).toString //启用日期
//      //      val zch = row.get(15).toString // 资产
//      val zk = row.get(10).toString //折扣
//      val fstr1 = row.get(6).toString //交易席位/公司代码
//      val key = zqlb + "-" + sh + "-" + fstr1 //证券类别+市场+交易席位/公司代码
//      val value = startDate + "-" + lv + "-" + zk + "-" + minLv //启用日期+利率+折扣+最低佣金值
//      (key, value)
//    }).groupByKey().mapValues(item => { //分组完成后进行排序取最大的启用日期的数据
//      item.toArray.sortWith((str1, str2) => {
//        str1.split("-")(0) > str2.split("-")(0)
//      })(0)
//    }).collectAsMap()

    //将参数表，利率表，佣金表进行广播

//    val csbValues = sc.broadcast(csbMap)
//    val lvbValues = sc.broadcast(flbMap)
//    val yjbValues = sc.broadcast(yjbMap)
        println(csbMap)
    val mm1=csbMap.keys.toList(0).substring(1,csbMap.keys.toList(0).length-1)
        println(csbMap.keys.toList(0).substring(1,csbMap.keys.toList(0).length-1))
    val mm2="117深圳中登计算数据费用处理模式值"
        println("117深圳中登计算数据费用处理模式值")
    println(mm1.equals(mm2))
        println(csbMap.keys.toList(0).length)
        println("117积数法计息自动产生付息凭证".length)

    println(csbMap.get(new String("117积数法计息自动产生付息凭证".getBytes("utf8"),"gbk")))
//        println(lvbValues.value)
//        println(lvbValues.value)


    import spark.implicits._

    //进行原始数据的计算
//    val value = df.rdd.map(row => {
//      val bcrq = row.getAs[String]("BCRQ") //本次日期
//      val zqdm = row.getAs[String]("ZQDM") //证券代码
//      val gsdm = row.getAs[String]("GSDM") //公司代码/交易席位
//      val gddm = row.getAs[String]("GDDM") //股东代码
//      val bs = row.getAs[String]("BS") //买卖
//      val sqbh = row.getAs[String]("SQBH") //申请编号
//      val key = bcrq + "-" + zqdm + "-" + gsdm + "-" + gddm + "-" + bs + "-" + sqbh
//      (key, row)
//    }).groupByKey()
//
//
//    val value1 = df.rdd.map(row => {
//      val bcrq = row.getAs[String]("BCRQ") //本次日期
//      val zqdm = row.getAs[String]("ZQDM") //证券代码
//      val gsdm = row.getAs[String]("GSDM") //公司代码/交易席位
//      val gddm = row.getAs[String]("GDDM") //股东代码
//      val bs = row.getAs[String]("BS") //买卖
//      val sqbh = row.getAs[String]("SQBH") //申请编号
//      val key = bcrq + "-" + zqdm + "-" + gsdm + "-" + gddm + "-" + bs
//      (key, row)
//    }).groupByKey()
//
//    //第一种  每一笔交易单独计算，最后相加
//    val fee1 = value1.map {
//      case (key, values) =>
//        val fields = key.split("-")
//        val bs = fields(4) //买卖方向
//      val gsdm = fields(2) //交易席位
//      val gddm = fields(3) //股东代码
//      val bcrq = fields(0) //本次日期
//      val zqdm = fields(1) //证券代码
//
//        //获取是否的参数
//        val cs1 = csbValues.value("117佣金包含经手费，证管费")
//        var cs2 = csbValues.value.getOrElse("上交所A股过户费按成交金额计算", "0")
//        if (!("0".equals(cs2) || ("1").equals(cs2))) {
//          //如果时日期格式的话要比较日期 TODO日期格式的比较
//          if (bcrq.compareTo(cs2) > 0) {
//            cs2 = "1"
//          } else {
//            cs2 = "0"
//          }
//        }
//        val cs3 = csbValues.value("117是否按千分之一费率计算过户费")
//        val cs4 = csbValues.value("117计算佣金减去风险金")
//        val cs5 = csbValues.value("117实际收付金额包含佣金")
//        val cs6 = csbValues.value("117计算佣金减去结算费")
//        val otherFee = BigDecimal(0)
//        /**
//          * 获取公共的费率
//          * key = 证券类别+市场+资产号+利率类别
//          * value = 启用日期+利率+折扣
//          * 获取费率时默认的资产为117如果没有则资产改为0，还没有则费率就取0
//          */
//        var rateJSstr = lvbValues.value.getOrElse("GP" + "-" + "S" + "-" + "117" + "-" + "JSF", "-1")
//        if ("-1".equals(rateJSstr)) rateJSstr = lvbValues.value.getOrElse("GP" + "-" + "S" + "-" + "0" + "-" + "JSF", "0-0-0")
//        val rateJS = rateJSstr.split("-")(1)
//        val rateJszk = rateJSstr.split("-")(2)
//
//        var rateYHStr = lvbValues.value.getOrElse("GP" + "-" + "S" + "-" + "117" + "-" + "YHS", "-1")
//        if ("-1".equals(rateYHStr)) rateYHStr = lvbValues.value.getOrElse("GP" + "-" + "S" + "0" + "YHS", "0-0-0")
//        val rateYH = rateYHStr.split("-")(1)
//        val rateYhzk = rateYHStr.split("-")(2)
//
//        var rateZGStr = lvbValues.value.getOrElse("GP" + "-" + "S" + "-" + "117" + "-" + "ZGF", "-1")
//        if ("-1".equals(rateZGStr)) rateZGStr = lvbValues.value.getOrElse("GP" + "-" + "S" + "-" + "0" + "-" + "ZGF", "0-0-0")
//        val rateZG = rateZGStr.split("-")(1)
//        val rateZgzk = rateZGStr.split("-")(2)
//
//        var rateGHStr = lvbValues.value.getOrElse("GP" + "-" + "S" + "-" + "117" + "-" + "GHF", "-1")
//        if ("-1".equals(rateGHStr)) rateGHStr = lvbValues.value.getOrElse("GP" + "-" + "S" + "-" + "0" + "-" + "GHF", "0-0-0")
//        val rateGH = rateGHStr.split("-")(1)
//        val rateGhzk = rateGHStr.split("-")(2)
//
//        var rateFXJStr = lvbValues.value.getOrElse("GP" + "-" + "S" + "-" + "117" + "-" + "FXJ", "-1")
//        if ("-1".equals(rateFXJStr)) rateFXJStr = lvbValues.value.getOrElse("GP" + "-" + "S" + "-" + "0" + "-" + "FXJ", "0-0-0")
//        val rateFXJ = rateFXJStr.split("-")(1)
//        val rateFxjzk = rateFXJStr.split("-")(2)
//
//        /**
//          * 获取佣金费率
//          * key=证券类别+市场+交易席位/公司代码
//          * value=启用日期+利率+折扣+最低佣金值
//          */
//        var rateYJStr = yjbValues.value.getOrElse("GP" + "-" + "S" + "-" + gddm, "-1")
//        if ("-1".eq(rateYJStr)) rateYJStr = yjbValues.value.getOrElse("GP" + "-" + "S" + "-" + gsdm, "0-0-0-0")
//        val rateYJ = rateYJStr.split("-")(1)
//        val rateYjzk = rateYJStr.split("-")(2)
//        val minYj = rateYjzk.split("-")(3)
//
//
//        var sumCjje = BigDecimal(0) //同一个申请编号总金额
//      var sumCjsl = BigDecimal(0) //同一个申请编号总数量
//      var sumYj = BigDecimal(0) //同一个申请编号总的佣金 （按成交记录）
//      var sumJsf = BigDecimal(0) //同一个申请编号总的经手费
//      var sumYhs = BigDecimal(0) //同一个申请编号总的印花税
//      var sumZgf = BigDecimal(0) //同一个申请编号总的征管费
//      var sumGhf = BigDecimal(0) //同一个申请编号总的过户费
//      var sumFxj = BigDecimal(0) //同一个申请编号总的风险金
//
//        for (row <- values) {
//          val cjje = BigDecimal(row.getAs[String]("CJJE"))
//          val cjsl = BigDecimal(row.getAs[String]("CJSL"))
//          //经手费的计算
//          val jsf = cjje * BigDecimal(rateJS) * BigDecimal(rateJszk).setScale(2, RoundingMode.HALF_UP)
//          var yhs = BigDecimal(0)
//          if ("S".equals(bs)) {
//            //印花税的计算
//            yhs = cjje.*(BigDecimal(rateYH)) * BigDecimal(rateYhzk).setScale(2, RoundingMode.HALF_UP)
//          }
//          //征管费的计算
//          val zgf = cjje.*(BigDecimal(rateZG)) * BigDecimal(rateZgzk).setScale(2, RoundingMode.HALF_UP)
//          //风险金的计算
//          val fx = cjje.*(BigDecimal(rateFXJ)) * BigDecimal(rateFxjzk).setScale(2, RoundingMode.HALF_UP)
//          //过户费的计算
//          var ghf = BigDecimal(0)
//          if ("1".equals(cs2)) {
//            if ("1".equals(cs3)) {
//              ghf = cjje * cjsl * BigDecimal(0.001).setScale(2, RoundingMode.HALF_UP)
//            } else {
//              ghf = cjje * cjsl * BigDecimal(rateGH) * BigDecimal(rateGhzk).setScale(2, RoundingMode.HALF_UP)
//            }
//          } else {
//            if ("1".equals(cs3)) {
//              ghf = cjsl * BigDecimal(0.001).setScale(2, RoundingMode.HALF_UP)
//            } else {
//              ghf = cjsl * BigDecimal(rateGH) * BigDecimal(rateGhzk).setScale(2, RoundingMode.HALF_UP)
//            }
//          }
//          //佣金的计算
//          var yj = cjje.*(BigDecimal(rateYJ)) * BigDecimal(rateYjzk).setScale(2, RoundingMode.HALF_UP)
//          if ("0".equals(cs1)) {
//            yj = yj - jsf - zgf
//          }
//          if ("1".equals(cs4)) {
//            yj = yj - fx
//          }
//
//          if ("1".equals(cs6)) {
//            yj = yj - otherFee
//          }
//          //单笔佣金小于最小佣金
//          if(yj-BigDecimal(minYj)<0){
//            yj = BigDecimal(minYj)
//          }
//
//          sumCjje = sumCjje.+(cjje)
//          sumCjsl = sumCjsl.+(cjsl)
//          sumYj = sumYj.+(yj)
//          sumJsf = sumJsf.+(jsf)
//          sumYhs = sumYhs.+(yhs)
//          sumZgf = sumZgf.+(zgf)
//          sumGhf = sumGhf.+(ghf)
//          sumFxj = sumFxj.+(fx)
//        }
//
//        (key, Fee("1",sumCjje, sumCjsl, sumYj, sumJsf, sumYhs, sumZgf,
//          sumGhf, sumFxj))
//    }
//
//
//    //第三种 金额汇总*费率
//    val fee3 = value1.map {
//      case (key, values) =>
//        val fields = key.split("-")
//        val bs = fields(4) //买卖方向
//      val gsdm = fields(2) //交易席位
//      val gddm = fields(3) //股东代码
//      val bcrq = fields(0) //本次日期
//      val zqdm = fields(1) //证券代码
//
//        //获取是否的参数
//        val cs1 = csbValues.value("117佣金包含经手费，证管费")
//        var cs2 = csbValues.value.getOrElse("上交所A股过户费按成交金额计算", "0")
//        if (!("0".equals(cs2) || ("1").equals(cs2))) {
//          //如果时日期格式的话要比较日期 TODO日期格式的比较
//          if (bcrq.compareTo(cs2) > 0) {
//            cs2 = "1"
//          } else {
//            cs2 = "0"
//          }
//        }
//        val cs3 = csbValues.value("117是否按千分之一费率计算过户费")
//        val cs4 = csbValues.value("117计算佣金减去风险金")
//        val cs5 = csbValues.value("117实际收付金额包含佣金")
//        val cs6 = csbValues.value("117计算佣金减去结算费")
//        val otherFee = BigDecimal(0)
//        /**
//          * 获取公共的费率
//          * key = 证券类别+市场+资产号+利率类别
//          * value = 启用日期+利率+折扣
//          * 获取费率时默认的资产为117如果没有则资产改为0，还没有则费率就取0
//          */
//        var rateJSstr = lvbValues.value.getOrElse("GP" + "-" + "S" + "-" + "117" + "-" + "JSF", "-1")
//        if ("-1".equals(rateJSstr)) rateJSstr = lvbValues.value.getOrElse("GP" + "-" + "S" + "-" + "0" + "-" + "JSF", "0-0-0")
//        val rateJS = rateJSstr.split("-")(1)
//        val rateJszk = rateJSstr.split("-")(2)
//
//        var rateYHStr = lvbValues.value.getOrElse("GP" + "-" + "S" + "-" + "117" + "-" + "YHS", "-1")
//        if ("-1".equals(rateYHStr)) rateYHStr = lvbValues.value.getOrElse("GP" + "-" + "S" + "0" + "YHS", "0-0-0")
//        val rateYH = rateYHStr.split("-")(1)
//        val rateYhzk = rateYHStr.split("-")(2)
//
//        var rateZGStr = lvbValues.value.getOrElse("GP" + "-" + "S" + "-" + "117" + "-" + "ZGF", "-1")
//        if ("-1".equals(rateZGStr)) rateZGStr = lvbValues.value.getOrElse("GP" + "-" + "S" + "-" + "0" + "-" + "ZGF", "0-0-0")
//        val rateZG = rateZGStr.split("-")(1)
//        val rateZgzk = rateZGStr.split("-")(2)
//
//        var rateGHStr = lvbValues.value.getOrElse("GP" + "-" + "S" + "-" + "117" + "-" + "GHF", "-1")
//        if ("-1".equals(rateGHStr)) rateGHStr = lvbValues.value.getOrElse("GP" + "-" + "S" + "-" + "0" + "-" + "GHF", "0-0-0")
//        val rateGH = rateGHStr.split("-")(1)
//        val rateGhzk = rateGHStr.split("-")(2)
//
//        var rateFXJStr = lvbValues.value.getOrElse("GP" + "-" + "S" + "-" + "117" + "-" + "FXJ", "-1")
//        if ("-1".equals(rateFXJStr)) rateFXJStr = lvbValues.value.getOrElse("GP" + "-" + "S" + "-" + "0" + "-" + "FXJ", "0-0-0")
//        val rateFXJ = rateFXJStr.split("-")(1)
//        val rateFxjzk = rateFXJStr.split("-")(2)
//
//        /**
//          * 获取佣金费率
//          * key=证券类别+市场+交易席位/公司代码
//          * value=启用日期+利率+折扣+最低佣金值
//          */
//        var rateYJStr = yjbValues.value.getOrElse("GP" + "-" + "S" + "-" + gddm, "-1")
//        if ("-1".eq(rateYJStr)) rateYJStr = yjbValues.value.getOrElse("GP" + "-" + "S" + "-" + gsdm, "0-0-0-0")
//        val rateYJ = rateYJStr.split("-")(1)
//        val rateYjzk = rateYJStr.split("-")(2)
//        val minYj = rateYjzk.split("-")(3)
//
//
//        var sumCjje = BigDecimal(0) //同一个申请编号总金额
//      var sumCjsl = BigDecimal(0) //同一个申请编号总数量
//
//
//        for (row <- values) {
//          val cjje = BigDecimal(row.getAs[String]("CJJE"))
//          val cjsl = BigDecimal(row.getAs[String]("CJSL"))
//          sumCjje = sumCjje.+(cjje)
//          sumCjsl = sumCjsl.+(cjsl)
//        }
//
//        var sumJsf2 = sumCjje * BigDecimal(rateJS) * BigDecimal(rateJszk).setScale(2, RoundingMode.HALF_UP)
//        //同一个申请编号总的经手费
//        var sumYhs2 = BigDecimal(0) //同一个申请编号总的印花税
//        if ("S".equals(bs)) {
//          sumYhs2 = sumCjje.*(BigDecimal(rateYH)) * BigDecimal(rateYhzk).setScale(2, RoundingMode.HALF_UP)
//        }
//        var sumZgf2 = sumCjje.*(BigDecimal(rateZG)) * BigDecimal(rateZgzk).setScale(2, RoundingMode.HALF_UP) //同一个申请编号总的征管费
//      var sumFxj2 = sumCjje.*(BigDecimal(rateFXJ)) * BigDecimal(rateFxjzk).setScale(2, RoundingMode.HALF_UP) //同一个申请编号总的风险金
//      //同一个申请编号总的过户费
//      var sumGhf2 = BigDecimal(0)
//        if ("1".equals(cs2)) {
//          if ("1".equals(cs3)) {
//            sumGhf2 = sumCjje * sumCjsl * BigDecimal(0.001).setScale(2, RoundingMode.HALF_UP)
//          } else {
//            sumGhf2 = sumCjje * sumCjsl * BigDecimal(rateGH) * BigDecimal(rateGhzk).setScale(2, RoundingMode.HALF_UP)
//          }
//        } else {
//          if ("1".equals(cs3)) {
//            sumGhf2 = sumCjsl * BigDecimal(0.001).setScale(2, RoundingMode.HALF_UP)
//          } else {
//            sumGhf2 = sumCjsl * BigDecimal(rateGH) * BigDecimal(rateGhzk).setScale(2, RoundingMode.HALF_UP)
//          }
//        }
//        //同一个申请编号总的佣金 （按申请编号汇总）
//        var sumYj2 = sumCjje.*(BigDecimal(rateYJ)) * BigDecimal(rateYjzk).setScale(2, RoundingMode.HALF_UP)
//        if ("0".equals(cs1)) {
//          sumYj2 = sumYj2 - sumJsf2 - sumZgf2
//        }
//        if ("1".equals(cs4)) {
//          sumYj2 = sumYj2 - sumFxj2
//        }
//
//        if ("1".equals(cs6)) {
//          sumYj2 = sumYj2 - otherFee
//        }
//
//        (key, Fee("3",sumCjje, sumCjsl, sumYj2, sumJsf2, sumYhs2, sumZgf2,
//          sumGhf2, sumFxj2))
//    }
//
//
//    //第二种 相同申请编号的金额汇总*费率，各申请编号汇总后的金额相加
//    val fee2 = value.map {
//      case (key, values) =>
//        val fields = key.split("-")
//        val bs = fields(4) //买卖方向
//        val gsdm = fields(2) //交易席位
//        val gddm = fields(3) //股东代码
//        val bcrq = fields(0) //本次日期
//        val zqdm = fields(1) //证券代码
//
//        //获取是否的参数
//        val cs1 = csbValues.value("117佣金包含经手费，证管费")
//        var cs2 = csbValues.value.getOrElse("上交所A股过户费按成交金额计算", "0")
//        if (!("0".equals(cs2) || ("1").equals(cs2))) {
//          //如果时日期格式的话要比较日期 TODO日期格式的比较
//          if (bcrq.compareTo(cs2) > 0) {
//            cs2 = "1"
//          } else {
//            cs2 = "0"
//          }
//        }
//        val cs3 = csbValues.value("117是否按千分之一费率计算过户费")
//        val cs4 = csbValues.value("117计算佣金减去风险金")
//        val cs5 = csbValues.value("117实际收付金额包含佣金")
//        val cs6 = csbValues.value("117计算佣金减去结算费")
//        val otherFee = BigDecimal(0)
//        /**
//          * 获取公共的费率
//          * key = 证券类别+市场+资产号+利率类别
//          * value = 启用日期+利率+折扣
//          * 获取费率时默认的资产为117如果没有则资产改为0，还没有则费率就取0
//          */
//        var rateJSstr = lvbValues.value.getOrElse("GP" + "-" + "S" + "-" + "117" + "-" + "JSF", "-1")
//        if ("-1".equals(rateJSstr)) rateJSstr = lvbValues.value.getOrElse("GP" + "-" + "S" + "-" + "0" + "-" + "JSF", "0-0-0")
//        val rateJS = rateJSstr.split("-")(1)
//        val rateJszk = rateJSstr.split("-")(2)
//
//        var rateYHStr = lvbValues.value.getOrElse("GP" + "-" + "S" + "-" + "117" + "-" + "YHS", "-1")
//        if ("-1".equals(rateYHStr)) rateYHStr = lvbValues.value.getOrElse("GP" + "-" + "S" + "0" + "YHS", "0-0-0")
//        val rateYH = rateYHStr.split("-")(1)
//        val rateYhzk = rateYHStr.split("-")(2)
//
//        var rateZGStr = lvbValues.value.getOrElse("GP" + "-" + "S" + "-" + "117" + "-" + "ZGF", "-1")
//        if ("-1".equals(rateZGStr)) rateZGStr = lvbValues.value.getOrElse("GP" + "-" + "S" + "-" + "0" + "-" + "ZGF", "0-0-0")
//        val rateZG = rateZGStr.split("-")(1)
//        val rateZgzk = rateZGStr.split("-")(2)
//
//        var rateGHStr = lvbValues.value.getOrElse("GP" + "-" + "S" + "-" + "117" + "-" + "GHF", "-1")
//        if ("-1".equals(rateGHStr)) rateGHStr = lvbValues.value.getOrElse("GP" + "-" + "S" + "-" + "0" + "-" + "GHF", "0-0-0")
//        val rateGH = rateGHStr.split("-")(1)
//        val rateGhzk = rateGHStr.split("-")(2)
//
//        var rateFXJStr = lvbValues.value.getOrElse("GP" + "-" + "S" + "-" + "117" + "-" + "FXJ", "-1")
//        if ("-1".equals(rateFXJStr)) rateFXJStr = lvbValues.value.getOrElse("GP" + "-" + "S" + "-" + "0" + "-" + "FXJ", "0-0-0")
//        val rateFXJ = rateFXJStr.split("-")(1)
//        val rateFxjzk = rateFXJStr.split("-")(2)
//
//        /**
//          * 获取佣金费率
//          * key=证券类别+市场+交易席位/公司代码
//          * value=启用日期+利率+折扣+最低佣金值
//          */
//        var rateYJStr = yjbValues.value.getOrElse("GP" + "-" + "S" + "-" + gddm, "-1")
//        if ("-1".eq(rateYJStr)) rateYJStr = yjbValues.value.getOrElse("GP" + "-" + "S" + "-" + gsdm, "0-0-0-0")
//        val rateYJ = rateYJStr.split("-")(1)
//        val rateYjzk = rateYJStr.split("-")(2)
//        val minYj = rateYjzk.split("-")(3)
//
//        var sumCjje = BigDecimal(0) //同一个申请编号总金额
//        var sumCjsl = BigDecimal(0) //同一个申请编号总数量
//
//
//        for (row <- values) {
//          val cjje = BigDecimal(row.getAs[String]("CJJE"))
//          val cjsl = BigDecimal(row.getAs[String]("CJSL"))
//          sumCjje = sumCjje.+(cjje)
//          sumCjsl = sumCjsl.+(cjsl)
//        }
//
//        var sumJsf2 = sumCjje * BigDecimal(rateJS) * BigDecimal(rateJszk).setScale(2, RoundingMode.HALF_UP)
//        //同一个申请编号总的经手费
//        var sumYhs2 = BigDecimal(0) //同一个申请编号总的印花税
//        if ("S".equals(bs)) {
//          sumYhs2 = sumCjje.*(BigDecimal(rateYH)) * BigDecimal(rateYhzk).setScale(2, RoundingMode.HALF_UP)
//        }
//        var sumZgf2 = sumCjje.*(BigDecimal(rateZG)) * BigDecimal(rateZgzk).setScale(2, RoundingMode.HALF_UP) //同一个申请编号总的征管费
//        var sumFxj2 = sumCjje.*(BigDecimal(rateFXJ)) * BigDecimal(rateFxjzk).setScale(2, RoundingMode.HALF_UP) //同一个申请编号总的风险金
//      //同一个申请编号总的过户费
//      var sumGhf2 = BigDecimal(0)
//        if ("1".equals(cs2)) {
//          if ("1".equals(cs3)) {
//            sumGhf2 = sumCjje * sumCjsl * BigDecimal(0.001).setScale(2, RoundingMode.HALF_UP)
//          } else {
//            sumGhf2 = sumCjje * sumCjsl * BigDecimal(rateGH) * BigDecimal(rateGhzk).setScale(2, RoundingMode.HALF_UP)
//          }
//        } else {
//          if ("1".equals(cs3)) {
//            sumGhf2 = sumCjsl * BigDecimal(0.001).setScale(2, RoundingMode.HALF_UP)
//          } else {
//            sumGhf2 = sumCjsl * BigDecimal(rateGH) * BigDecimal(rateGhzk).setScale(2, RoundingMode.HALF_UP)
//          }
//        }
//        //同一个申请编号总的佣金 （按申请编号汇总）
//        var sumYj2 = sumCjje.*(BigDecimal(rateYJ)) * BigDecimal(rateYjzk).setScale(2, RoundingMode.HALF_UP)
//        if ("0".equals(cs1)) {
//          sumYj2 = sumYj2 - sumJsf2 - sumZgf2
//        }
//        if ("1".equals(cs4)) {
//          sumYj2 = sumYj2 - sumFxj2
//        }
//
//        if ("1".equals(cs6)) {
//          sumYj2 = sumYj2 - otherFee
//        }
//
//        (bcrq + "-" + zqdm + "-" + gsdm + "-" + gddm + "-" + bs,
//          Fee("2",sumCjje, sumCjsl, sumYj2, sumJsf2, sumYhs2, sumZgf2,
//            sumGhf2, sumFxj2))
//    }.groupByKey().map {
//      case (key, fees) =>
//        var totalYj2 = BigDecimal(0)
//        var totalJsf2 = BigDecimal(0)
//        var totalYhs2 = BigDecimal(0)
//        var totalZgf2 = BigDecimal(0)
//        var totalGhf2 = BigDecimal(0)
//        var totalFxj2 = BigDecimal(0)
//        var totalCjje = BigDecimal(0)
//        var totalCjsl = BigDecimal(0)
//        for (fee <- fees) {
//          totalCjje += fee.sumCjje
//          totalCjsl += fee.sumCjsl
//          totalYj2 += fee.sumYj
//          totalJsf2 += fee.sumJsf
//          totalYhs2 += fee.sumYhs
//          totalZgf2 += fee.sumZgf
//          totalGhf2 += fee.sumGhf
//          totalFxj2 += fee.sumFxj
//        }
//        (key, Fee("2",totalCjje, totalCjsl, totalYj2, totalJsf2, totalYhs2, totalZgf2,
//          totalGhf2, totalFxj2))
//    }
//
//    val middle = fee1.union(fee2).union(fee3)
//
//    val result = middle.groupByKey().map {
//      case (key, fees) =>
//        val fields = key.split("-")
//        val bs = fields(4)
//
//        var totalYj2 = BigDecimal(0)
//        var totalJsf2 = BigDecimal(0)
//        var totalYhs2 = BigDecimal(0)
//        var totalZgf2 = BigDecimal(0)
//        var totalGhf2 = BigDecimal(0)
//        var totalFxj2 = BigDecimal(0)
//
//        var totalCjje = BigDecimal(0)
//        var totalCjsl = BigDecimal(0)
//
//        var totalYj1 = BigDecimal(0)
//        var totalJsf1 = BigDecimal(0)
//        var totalYhs1 = BigDecimal(0)
//        var totalZgf1 = BigDecimal(0)
//        var totalGhf1 = BigDecimal(0)
//        var totalFxj1 = BigDecimal(0)
//
//        var totalYj3 = BigDecimal(0)
//        var totalJsf3 = BigDecimal(0)
//        var totalYhs3 = BigDecimal(0)
//        var totalZgf3 = BigDecimal(0)
//        var totalGhf3 = BigDecimal(0)
//        var totalFxj3 = BigDecimal(0)
//
//        for(fee <- fees){
//          if("1".equals(fee.ctype)){
//            totalCjje = fee.sumCjje
//            totalCjsl = fee.sumCjsl
//            totalYj1 = fee.sumYj
//            totalJsf1 = fee.sumJsf
//            totalYhs1 = fee.sumYhs
//            totalZgf1 = fee.sumZgf
//            totalGhf1 = fee.sumGhf
//            totalFxj1 = fee.sumFxj
//          }else if("2".equals(fee.ctype)){
//            totalYj2 = fee.sumYj
//            totalJsf2 = fee.sumJsf
//            totalYhs2 = fee.sumYhs
//            totalZgf2 = fee.sumZgf
//            totalGhf2 = fee.sumGhf
//            totalFxj2 = fee.sumFxj
//          }else if("3".equals(fee.ctype)){
//            totalYj3 = fee.sumYj
//            totalJsf3 = fee.sumJsf
//            totalYhs3 = fee.sumYhs
//            totalZgf3 = fee.sumZgf
//            totalGhf3 = fee.sumGhf
//            totalFxj3 = fee.sumFxj
//          }
//        }
//
//        //判断取值逻辑
//
//        var realYj = BigDecimal(0)
//        var realJsf = BigDecimal(0)
//        var realYhs = BigDecimal(0)
//        var realZgf = BigDecimal(0)
//        var realGhf = BigDecimal(0)
//        var realFxj = BigDecimal(0)
//
//
//        val con1 = csbValues.value("117按申请编号汇总计算经手费")
//        val con2 = csbValues.value("117按申请编号汇总计算征管费")
//        val con3 = csbValues.value("117按申请编号汇总计算过户费")
//        val con4 = csbValues.value("117按申请编号汇总计算印花税")
//        val con5 = csbValues.value("117S按申请编号汇总计算佣金")
//        val con7 = csbValues.value("117S按申请编号汇总计算风险金")
//        val con8 = csbValues.value("117实际收付金额包含佣金")
//
//        val con11 = csbValues.value("117按成交记录计算经手费")
//        val con12 = csbValues.value("117按成交记录计算征管费")
//        val con13 = csbValues.value("117按成交记录计算过户费")
//        val con14 = csbValues.value("117按成交记录计算印花税")
//        val con15 = csbValues.value("117S按成交记录计算佣金")
//        val con17 = csbValues.value("117S按成交记录计算风险金")
//
//        if ("1".equals(con1)) {
//          realJsf = totalJsf2
//        } else if ("1".equals(con11)) {
//          realJsf = totalJsf1
//        }else{
//          realJsf = totalJsf3
//        }
//
//        if ("1".equals(con2)) {
//          realZgf = totalZgf2
//        } else if ("1".equals(con12)) {
//          realZgf = totalZgf1
//        }else{
//          realZgf = totalZgf3
//        }
//
//        if ("1".equals(con3)) {
//          realGhf = totalGhf2
//        } else if ("1".equals(con13)) {
//          realGhf = totalGhf1
//        }else{
//          realGhf = totalGhf3
//        }
//
//        if ("1".equals(con4)) {
//          realYhs = totalYhs2
//        } else if ("1".equals(con14)) {
//          realYhs = totalYhs1
//        }else{
//          realYhs = totalYhs3
//        }
//        if ("1".equals(con5)) {
//          realYj = totalYj2
//        } else if ("1".equals(con15)) {
//          realYj = totalYj1
//        }else{
//          realYj = totalYj3
//        }
//
//        if ("1".equals(con7)) {
//          realFxj = totalFxj2
//        } else if ("1".equals(con17)) {
//          realFxj = totalFxj1
//        }else{
//          realFxj = totalFxj3
//        }
//
//        var FBje = BigDecimal(0)
//        var FSje = BigDecimal(0)
//        var FBsl = BigDecimal(0)
//        var FSsl = BigDecimal(0)
//        var FByj = BigDecimal(0)
//        var FSyj = BigDecimal(0)
//        var FBjsf = BigDecimal(0)
//        var FSjsf = BigDecimal(0)
//        var FByhs = BigDecimal(0)
//        var FSyhs = BigDecimal(0)
//        var FBzgf = BigDecimal(0)
//        var FSzgf = BigDecimal(0)
//        var FBghf = BigDecimal(0)
//        var FSghf = BigDecimal(0)
//        val FBgzlx = BigDecimal(0)
//        val FSgzlx = BigDecimal(0)
//        var FBFxj = BigDecimal(0)
//        var FSFxj = BigDecimal(0)
//
//        /**
//          * var realYj = totalYj
//          * var realJsf = totalJsf
//          * var realYhs = totalYhs
//          * var realZgf = totalZgf
//          * var realGhf = totalGhf
//          * var realFxj = totalFxj
//          */
//        if ("B".equals(bs)) {
//          FBje = totalCjje
//          FBsl = totalCjsl
//          FBjsf = realJsf
//          //          FByhs = sumYhs.setScale(2, RoundingMode.HALF_UP)
//          FBzgf = realZgf
//          FBghf = realGhf
//          FBFxj = realFxj
//          FByj = realYj
//        } else {
//          FSje = totalCjje
//          FSsl = totalCjsl
//          FSjsf = realJsf
//          FSyhs = realYhs
//          FSzgf = realZgf
//          FSghf = realGhf
//          FSFxj = realFxj
//          FSyj = realYj
//        }
//        val bcrq = fields(0)
//        val FSzsh = "H"
//        val Fjyxwh = fields(2)
//        var FBsfje = FBje.+(FBjsf).+(FBzgf).+(FBghf)
//        var FSssje = FSje.-(FSjsf).-(FSzgf).-(FSghf).-(FSyhs)
//        if ("1".equals(con8)) {
//          FBsfje += FByj
//          FSssje -= FByj
//        }
//        val FZqbz = "GP"
//        val FYwbz = "DZ"
//        val FQsbz = "N"
//        val FBQtf = BigDecimal(0)
//        val FSQtf = BigDecimal(0)
//        val ZqDm = fields(1)
//        val FJyFS = "PT"
//        val Fsh = "1"
//        val Fzzr = " "
//        val Fchk = " "
//        val fzlh = "0"
//        val ftzbz = ""
//        val FBQsghf = BigDecimal(0)
//        val FSQsghf = BigDecimal(0)
//        val FGddm = fields(3)
//        val FHGGAIN = BigDecimal(0)
//
//        //bcrq + "-" + zqdm + "-" + gsdm + "-" + gddm + "-" + bs,
//        DGH000013(bcrq, bcrq, ZqDm, FSzsh, Fjyxwh, FBje.formatted("%.2f"), FSje.formatted("%.2f"), FBsl.formatted("%.2f"), FSsl.formatted("%.2f"), FByj.formatted("%.2f"),
//          FSyj.formatted("%.2f"), FBjsf.formatted("%.2f"), FSjsf.formatted("%.2f"), FByhs.formatted("%.2f"), FSyhs.formatted("%.2f"), FBzgf.formatted("%.2f"), FSzgf.formatted("%.2f"), FBghf.formatted("%.2f"), FSghf.formatted("%.2f"), FBgzlx.formatted("%.2f"),
//          FSgzlx.formatted("%.2f"), FHGGAIN.formatted("%.2f"), FBFxj.formatted("%.2f"), FSFxj.formatted("%.2f"), FBsfje.formatted("%.2f"), FSssje.formatted("%.2f"), FZqbz, FYwbz, FQsbz, FBQtf.formatted("%.2f"), FSQtf.formatted("%.2f"),
//          ZqDm, FJyFS, Fsh, Fzzr, Fchk, fzlh, ftzbz, FBQsghf.formatted("%.2f"), FSQsghf.formatted("%.2f"), FGddm)
//    }
//

//       Util.outputMySql(result.toDF(), "SHDZGH3")
    spark.stop()

  }

}
