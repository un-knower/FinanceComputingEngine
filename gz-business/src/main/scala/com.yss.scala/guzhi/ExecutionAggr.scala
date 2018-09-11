package com.yss.scala.guzhi

import java.sql.DriverManager

import com.yss.scala.dto.{ExecutionAggrObj}
import com.yss.scala.guzhi.ExecutionContants._
import com.yss.scala.guzhi.ShghContants.{DEFORT_VALUE1, DEFORT_VALUE2, DEFORT_VALUE3, NO, SALE, SEPARATE1, YES, ZCLB}
import com.yss.scala.util.Util
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.math.BigDecimal.RoundingMode
/**
  * @author ChenYao
  * date 2018/8/8
  *
  *原始数据表:execution_aggr_N000032F0001_1_20160825.tsv
  *参数表:
  *佣金表:
  *
  *
  *数据处理:{日期 +SecurityID +市场(S)+交易席位(ReportingPBUID)+SIDE(买卖)+股东代码(AccountID)} 相同的进行买入和卖出的计算
  *         输出到mysql数据库JJCWGZ 的表 SJSV5中
  */
object ExecutionAggr {
  def main(args: Array[String]): Unit = {


    /**
      * 1.读取 原始数据表、参数表、佣金利率表、费率表
      */
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SJSV5")
    val sc = new SparkContext(sparkConf)
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val exe = sc.textFile("C:\\Users\\hgd\\Desktop\\jiaoyan\\execution_aggr_F000995F0401_1_20180808.tsv")
    //参数表,编码格式的转换
    val csb = sc.hadoopFile("C:\\Users\\hgd\\Desktop\\过户\\参数.csv", classOf[TextInputFormat], classOf[LongWritable], classOf[Text])
      .map(pair => new String(pair._2.getBytes, 0, pair._2.getLength, "GBK"))
    //费率表
    val flb = Util.readCSV("C:\\Users\\hgd\\Desktop\\过户\\交易利率.csv", sparkSession)
    //佣金表
    val yjb = Util.readCSV("C:\\Users\\hgd\\Desktop\\过户\\佣金利率.csv", sparkSession)

    //交易费用表（佣金的三种模式）
    val  jyfy= Util.readCSV("C:\\Users\\hgd\\Desktop\\过户\\A117csxwfy.csv", sparkSession)

    //读取数据库CSQSXW表，席位表
    val xwb=sparkSession.read.format("jdbc")
      .option("url","jdbc:oracle://192.168.102.68:1521/employee")
      .option("dbtable","CSQSXW")
      .option("user","hadoop")
      .option("password","hadoop")
      .load()


    xwb.createOrReplaceTempView("CSQSXW")
    val xwValue= sparkSession.sql(" select FQSDM,FQSXW from CSQSXW where  FSZSH='S' and  FSETCODE=117") //得出117套账号的席位号ReportingPBUID





    /**
      * 2 .取出参数表中的 （套账号 + 中文名， 启用不启用)
      *
      */
    val csbMap = csb.map(row => {
      val fields = row.replaceAll(""""""", "").split(SEPARATE2)
      val key = fields(0)
      //参数名
      val value = fields(1) //参数值
      (key, value)
    }).collectAsMap()


    /**
      * 3.编写参数，用来和参数表进行比对，进行取值
      *
      */

    val cs1 = csbMap(CS1_KEY) //是否开启佣金包含经手费，证管费
    val cs4 = csbMap(CS4_KEY) //启计算佣金减去风险金
    val cs6 = csbMap(CS6_KEY) //计算佣金减去结算费
    val cs7 = csbMap(CON18_KEY) //深圳佣金计算保留位数

    val cs8 = csbMap(CON19_KEY) //交易所国债以全价计提佣金
    val cs9 = csbMap(CON20_KEY) //交易所非国债以净价计提佣金
    val cs10 = csbMap(CON21_KEY) //深交所证管费和经手
    val cs11=csbMap(CON22_KEY) //指数、指标股票按特殊科目设置页面处理




    //获取计算参数
    val con1 = csbMap(CON1_KEY) //是否勾选按申请编号汇总计算经手费
    val con2 = csbMap(CON2_KEY) //是否勾选按申请编号汇总计算征管费
    val con3 = csbMap(CON3_KEY) //是否勾选按申请编号汇总计算过户费
    val con4 = csbMap(CON4_KEY) //是否勾选按申请编号汇总计算印花税
    val con5 = csbMap(CON5_KEY) //是否勾选H按申请编号汇总计算佣金
    val con7 = csbMap(CON7_KEY) //是否勾选H按申请编号汇总计算风险金
    val con8 = csbMap(CON8_KEY) //是否开启实际收付金额包含佣金

    val con11 = csbMap(CON11_KEY) //是否开启按成交记录计算经手费
    val con12 = csbMap(CON12_KEY) //是否开启按成交记录计算征管费
    val con13 = csbMap(CON13_KEY) //是否开启按成交记录计算过户费
    val con14 = csbMap(CON14_KEY) //是否开启按成交记录计算印花税
    val con15 = csbMap(CON15_KEY) //是否开启H按成交记录计算佣金
    val con17 = csbMap(CON17_KEY) //是否开启H按成交记录计算风险金




    /**
      * 4.将费率变成map结构
      * key: 资产号+ 证券类别( 先取业务标示，取不到取证券类别)+市场+利率类别  117 GP S JSF
      * value: 启用日期+利率+折扣
      * 排序: 按照日期进行排序 从大到小排序
      */
    val flbMapValue = flb.rdd.map(row => {
      val zqlb = row.get(0).toString
      //证券类别
      val sh = row.get(1).toString
      //市场
      val lvlb = row.get(2).toString
      //利率类别
      val lv = row.get(3).toString //利率
      val zk = row.get(5).toString //折扣
      val zch = row.get(10).toString //资产号  117或者0
      val startDate = row.get(13).toString
      //启用日期 ZCLB + SEPARATE1 + SH + SEPARATE1 + ZYZCH + SEPARATE1 + JSF, DEFORT_VALUE1
      val key = zqlb + SEPARATE1 + sh + SEPARATE1 + zch + SEPARATE1 + lvlb //证券类别+市场+资产号+利率类别
      val value = startDate + SEPARATE1 + lv + SEPARATE1 + zk //启用日期+利率+折扣
      (key, value)
    }).groupByKey().mapValues(item => { //分组完成后进行排序
      item.toArray.sortWith((str1, str2) => {
        str1.split(SEPARATE1)(0).compareTo(str2.split(SEPARATE1)(0)) > 0
      })
    }).collectAsMap()    //(key,Array(从大到小))


    /**
      * 5.将佣金变成map结构
      * key : 证券类别@市场@交易席位   GP  S  交易席位号
      * value: 启用日期@利率@折扣@最低佣金值
      * 从小到大排序
      */
    val yjbMapValues = yjb.rdd.map(row => {
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
        str1.split(SEPARATE1)(0).compareTo(str2.split(SEPARATE1)(0)) > 0
      })
    }).collectAsMap()



    /**
      * 6.将佣金和利率表进行广播
      */
    val yjbValues = sc.broadcast(yjbMapValues)
    val flbValues = sc.broadcast(flbMapValue)



    /**
      * 获取费率 和佣金
      * key = 证券类别+市场+资产号+利率类别
      * value = 启用日期+利率+折扣
      * 获取费率时默认的资产为117如果没有则资产改为0，还没有则费率就取0
      */

    def getRate(gsdm: String,bcrq:String) = {

      //将现在的日期跟费率表中的进行比较，取小于当前日期的最大日期
      val flbMap = flbValues.value.mapValues(items => items.partition(_.split(SEPARATE1)(0).compareTo(bcrq) <= 0)._1(0))
      //将现在的日期跟佣金表中的进行比较，取小于当前日期的最大日期
      val yjMap = yjbValues.value.mapValues(items => items.partition(_.split(SEPARATE1)(0).compareTo(bcrq) <= 0)._1(0))


      //Map.getOrElse 如果有值的话取到，没有取到值的话，赋值-1
      var rateJSstr = flbMap.getOrElse(ZCLB + SEPARATE1 + SH + SEPARATE1 + ZYZCH + SEPARATE1 + JSF, DEFORT_VALUE1)  //判断包含117的这个key有没有
      if (DEFORT_VALUE1.equals(rateJSstr)) rateJSstr = flbMap.getOrElse(ZCLB + SEPARATE1 + SH + SEPARATE1 + GYZCH + SEPARATE1 + JSF, DEFORT_VALUE2) //如果还没有，将费率设置为0
      val rateJS = rateJSstr.split(SEPARATE1)(1) //取经管费利率
      val rateJszk = rateJSstr.split(SEPARATE1)(2) //取经管费折扣

      var rateYHStr = flbMap.getOrElse(ZCLB + SEPARATE1 + SH + SEPARATE1 + ZYZCH + SEPARATE1 + YHS, DEFORT_VALUE1)
      if (DEFORT_VALUE1.equals(rateYHStr)) rateYHStr = flbMap.getOrElse(ZCLB + SEPARATE1 + SH + SEPARATE1 + GYZCH + SEPARATE1 + YHS, DEFORT_VALUE2)
      val rateYH = rateYHStr.split(SEPARATE1)(1) //印花费利率
      val rateYhzk = rateYHStr.split(SEPARATE1)(2) //印花费折扣

      var rateZGStr = flbMap.getOrElse(ZCLB + SEPARATE1 + SH + SEPARATE1 + ZYZCH + SEPARATE1 + ZGF, DEFORT_VALUE1)
      if (DEFORT_VALUE1.equals(rateZGStr)) rateZGStr = flbMap.getOrElse(ZCLB + SEPARATE1 + SH + SEPARATE1 + GYZCH + SEPARATE1 + ZGF, DEFORT_VALUE2)
      val rateZG = rateZGStr.split(SEPARATE1)(1) //征管费利率
      val rateZgzk = rateZGStr.split(SEPARATE1)(2) //征管费折扣

      var rateGHStr = flbMap.getOrElse(ZCLB + SEPARATE1 + SH + SEPARATE1 + ZYZCH + SEPARATE1 + GHF, DEFORT_VALUE1)
      if (DEFORT_VALUE1.equals(rateGHStr)) rateGHStr = flbMap.getOrElse(ZCLB + SEPARATE1 + SH + SEPARATE1 + GYZCH + SEPARATE1 + GHF, DEFORT_VALUE2)
      val rateGH = rateGHStr.split(SEPARATE1)(1) //过户费利率
      val rateGhzk = rateGHStr.split(SEPARATE1)(2)   //过户费折扣

      var rateFXJStr = flbMap.getOrElse(ZCLB + SEPARATE1 + SH + SEPARATE1 + ZYZCH + SEPARATE1 + FXJ, DEFORT_VALUE1)
      if (DEFORT_VALUE1.equals(rateFXJStr)) rateFXJStr = flbMap.getOrElse(ZCLB + SEPARATE1 + SH + SEPARATE1 + GYZCH + SEPARATE1 + FXJ, DEFORT_VALUE2)
      val rateFXJ = rateFXJStr.split(SEPARATE1)(1) //风险金利率
      val rateFxjzk = rateFXJStr.split(SEPARATE1)(2) //风险金折扣

      /**
        * 获取佣金费率
        * key=证券类别+市场+交易席位/股东代码
        * value=启用日期+利率+折扣+最低佣金值
        */
      // var rateYJStr = yjbValues.value.getOrElse(ZCLB + SEPARATE1 + SH + SEPARATE1 + gddm, DEFORT_VALUE1)
      // if (DEFORT_VALUE1.eq(rateYJStr))
      val rateYJStr = yjMap.getOrElse(ZCLB + SEPARATE1 + SH + SEPARATE1 + gsdm, DEFORT_VALUE3)
      val rateYJ = rateYJStr.split(SEPARATE1)(1)  //利率
      val rateYjzk = rateYJStr.split(SEPARATE1)(2) //折扣率
      val minYj = rateYJStr.split(SEPARATE1)(3) //最低佣金值



      (rateJS,rateJszk,rateYH,rateYhzk,rateZG,rateZgzk,rateGH,rateGhzk,rateFXJ,rateFxjzk,rateYJ, rateYjzk, minYj)
    }




    /** 8.原数据的转换
      * 进行map,将数据按 "\t" 分割
      * key:日期 +SecurityID +市场(S)+交易席位(ReportingPBUID)+SIDE(买卖)+股东代码(AccountID)
      * value: 所有
      */
    val map1 = exe.map {
      case (text) => {
        val word = text.split("\t")
        val ReportingPBUID = word(3) //回报交易单元
        val SecurityID = word(5) //证券代码
        val TransactTime1 = word(9)
        val TransactTime=TransactTime1.substring(0,8) //回报时间
        val Side = word(20) //买卖方向
        val Market="S"
        val AccountID=word(21)
        val key=TransactTime+SEPARATE1+SecurityID+SEPARATE1+Market+SEPARATE1+ReportingPBUID+SEPARATE1+Side+SEPARATE1+AccountID
        (key,text)  //key  日期 +SecurityID +市场(S)+交易席位(ReportingPBUID)+SIDE(买卖)+股东代码(AccountID)
      }
    }.groupByKey()


    /** 9.原数据的转换
      * 进行map,将数据按 "\t" 分割
      * key:日期 +SecurityID +市场(S)+交易席位(ReportingPBUID)+SIDE(买卖)+股东代码(AccountID)+申请编号
      * value: 所有
      */
    /*
        val map2 = exe.map {
          case (text) => {
            val word = text.split("\t")
            val ReportingPBUID = word(3) //回报交易单元
            val SecurityID = word(5) //证券代码
            val TransactTime1 = word(9)
            val TransactTime=TransactTime1.substring(0,8) //回报时间
            val Side = word(20) //买卖方向
            val Market="S"
            val key=TransactTime+"_"+SecurityID+"_"+Market+"_"+ReportingPBUID+"_"+Side+"申请编号"
            (key,text)  //key  日期 +证券代码(SecurityID) +市场(S)+交易席位(ReportingPBUID)+SIDE(买卖)+股东代码(AccountID)
          }
        }.groupByKey()*/


    /**
      * 方法: 输入 股东代码/席位号  证券标志 业务标志  交易费用
      *
      */

    def getYj(gddm:String,ReportingPBUID:String,FZqbz:String,Fywbz:String ,sumYj:BigDecimal,sumJSF:BigDecimal,sumZGF:BigDecimal){
      //同一个席位号只能选一个JSF,ZGF
      val jyfyMap= jyfy.rdd.map{
        case(row)=>{
          val gddm1=row.get(0).toString  //股东代码
          val zqlb=row.get(2).toString  //证券类别
          val ffylb=row.get(3).toString //费用类别 JSF
          val ffyfs=row.get(4).toString //费用方式  券商承担 产品承担 计入成本
          val key=gddm+SEPARATE1+zqlb+SEPARATE1+ffylb
          val value=ffyfs
          (key,value)
        }
      }.collectAsMap()

      var JSF=  jyfyMap.getOrElse(gddm+SEPARATE1+FZqbz+SEPARATE1+"JSF","-1")  //如果这个key的话，取得这个值，没有取-1
      var ZGF=  jyfyMap.getOrElse(gddm+SEPARATE1+FZqbz+SEPARATE1+"ZGF","-1")
      var JSF1=  jyfyMap.getOrElse(ReportingPBUID+SEPARATE1+FZqbz+SEPARATE1+"JSF","-1")
      var ZGF1=  jyfyMap.getOrElse(ReportingPBUID+SEPARATE1+FZqbz+SEPARATE1+"ZGF","-1")
      var JSF2=  jyfyMap.getOrElse(ReportingPBUID+SEPARATE1+Fywbz+SEPARATE1+"JSF","-1")
      var ZGF2=  jyfyMap.getOrElse(ReportingPBUID+SEPARATE1+Fywbz+SEPARATE1+"ZGF","-1")

      var sumYj1=BigDecimal(0)

      if(JSF == 0 || JSF1 == 0 || JSF == 0){ //有经手费，有证管费

        if(ZGF == 0 || ZGF1 == 0 || ZGF2 == 0){//有经手费，有证管费

          //佣金-征管费-经手费
          sumYj1=sumYj-sumJSF-sumZGF
        }else{//有经手费，没证管费
          //佣金-经手费
          sumYj1=sumYj-sumJSF
        }
      }else if (ZGF == 0 || ZGF1 == 0 || ZGF2 == 0){  //没有经手费 ，有证管费
        sumYj1=sumYj-sumZGF
      }
      sumYj1
    }


    /**
      *  处理业务标志 Fywbz  证券标志 Fzqbz 两个字段
      *key:   日期 +证券代码(SecurityID) +市场(S)+交易席位(ReportingPBUID)+SIDE(买卖)+股东代码(AccountID)
      *
      *value: LastPx 成交价    LastQty 成交数量      Fywbz 业务标志  Fzqbz 业务标志
      *
      1.参数启用:指数、指标股票按特殊科目设置页面处理 &&(通过席位查询席位表 CSQSXW 判断是否指数席位 fqsxw||通过特殊参数页面表(CsTsKm)判断是指数股票)取值为ZS
      2.参数启用:指数、指标股票按特殊科目设置页面处理 &&通过特殊参数页面表(CsTsKm)判断是指标股票取值为ZB
      3.LSetCsSysJj表中FJJLX=0 (WHERE FSETCODE=套帐号)&&LSetCsSysJj表中FJJLB=1||5||7 (WHERE FSETCODE=套帐号)&& (V5文件中的gsdm字段在CsQsXw表中有数据 || zqdm字段在CsTsKm表中有数据 取值为     ZS
     4.LSetCsSysJj表中FJJLX=0 (WHERE FSETCODE=套帐号)&& LSetCsSysJj表中FJJLB=2 (WHERE FSETCODE=套帐号)&& zqdm字段在CsTsKm表中有数据     取值为ZB
      *
      */
    map1.map{
      case(key,values)=>{

        val fields = key.split(SEPARATE1)
        val bs = fields(4) //买卖方向
        val gsdm = fields(3) //交易席位
        val bcrq = fields(0) //本次日期
        val zqdm = fields(1) //证券代码
        val ggdm=fields(5) //股东代码
        val LastPx = BigDecimal(fields(16)) //成交价
        val LastQty = BigDecimal(fields(17)) //成交数量










      }
    }





    /**
      * 计算金额的费率 和佣金
      * 按照成交记录计算  ，费率每一笔单独计算最后相加，佣金进行汇总，总的费率*总的金额

      *
      *
      */
    val fee1 = map1.map {
      case (key, values) => {
        val fields = key.split(SEPARATE1)
        val bs = fields(4) //买卖方向
        val gsdm = fields(3) //交易席位
        val bcrq = fields(0) //本次日期
        val zqdm = fields(1) //证券代码
        val ggdm=fields(5) //股东代码


        val getRateResult = getRate(gsdm,bcrq)
        val rateJS:String = getRateResult._1
        val rateJszk:String = getRateResult._2
        val rateYH:String = getRateResult._3
        val rateYhzk:String = getRateResult._4
        val rateZG:String = getRateResult._5
        val rateZgzk:String = getRateResult._6
        val rateGH:String = getRateResult._7
        val rateGhzk:String = getRateResult._8
        val rateFXJ:String = getRateResult._9
        val rateFxjzk:String = getRateResult._10
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

        for (row <- values) {

          val text = row.split("\t")
          val cjje = BigDecimal(text(16))  //金额
          val cjsl = BigDecimal(text(17)) //数量

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
          val  ghf =cjje.*(BigDecimal(rateGH)).*(BigDecimal(rateGhzk)).setScale(2, RoundingMode.HALF_UP)

          sumCjje = sumCjje.+(cjje)
          sumCjsl = sumCjsl.+(cjsl)
          //sumYj = sumYj.+(yj)
          sumJsf = sumJsf.+(jsf)
          sumYhs = sumYhs.+(yhs)
          sumZgf = sumZgf.+(zgf)
          sumGhf = sumGhf.+(ghf)
          sumFxj = sumFxj.+(fx)
        }

        //佣金的计算
        sumYj = sumCjje.*(BigDecimal(rateYJ)).*(BigDecimal(rateYjzk)).setScale(2, RoundingMode.HALF_UP)
        if (NO.equals(cs1)) {    // cs1 是否包含经手费，证管费 ，
          sumYj = sumYj.-(sumJsf).-(sumZgf)
        }else{

          // 股东代码/席位号  证券标志/业务标志





        }

        if (YES.equals(cs4)) {  // cs4 是否计算佣金减去风险金
          sumYj = sumYj.-(sumFxj)
        }
        if (YES.equals(cs6)) { // cs6 是否计算佣金减去结算费    启用： 佣金-金额*经手费率
          sumYj = sumYj.-(sumCjje.*(sumJsf))
        }






        if (sumYj < BigDecimal(minYj)) {
          sumYj = BigDecimal(minYj)
        }

       // (key, SJSObj("1", sumCjje, sumCjsl, sumYj, sumJsf, sumYhs, sumZgf,
       //   sumGhf, sumFxj))
      }
    }

    /**
      * 11.根据申请编号金额汇总*费率，佣金进行汇总，再根据没有申请编号的key进行groupByKey将金额费率进行相加
      *
      */


    /**
      * 12.金额汇总*费率，佣金进行汇总（进行判断是否开启）
      */




    /**
      * 13.将三种进行join => (key,(相同key的三种情况))
      *
      */

    /**
      * 14.进行map,判断参数是否开启，开启就取得不同的费率，根据参数是否开启算不同的费率
      *
      */








    //4.进行计算
    val finallData= map1.flatMap{
      case(key,iterable)=>{
        var Fbj =BigDecimal(0)

        var Fbje = BigDecimal(0)//买金额
        var Fsje = BigDecimal(0) //卖金额
        var FBsl = BigDecimal(0) //买数量
        var FSsl = BigDecimal(0) //卖数量
        var Fbyj = BigDecimal(0) //买佣金
        var Fsyj = BigDecimal(0) //卖佣金
        var FBjsf = BigDecimal(0) //买经手费
        var FSjsf = BigDecimal(0) //卖经手费
        var Fbyhs = BigDecimal(0) //买印花税
        var Fsyhs = BigDecimal(0) //卖印花税
        var FBzgf = BigDecimal(0) //买征管费
        var FSzgf = BigDecimal(0) //卖征管费
        var FBghf = BigDecimal(0) //买过户费
        var FSghf = BigDecimal(0) //卖过户费
        var FBfxj = BigDecimal(0) //买风险金
        var FSfxj = BigDecimal(0) //卖风险金
        var Fbsfje=BigDecimal(0) //买实付金额
        var Fsssje=BigDecimal(0) //卖实收金额



        //费率 1.佣金费率，经手费率，印花费率，过户费率，证管费率，风险金费率
        val commisionRate =BigDecimal( 0.0025)//佣金费率
        val HandingFeeRate = BigDecimal(0.000148) //经手费率
        val PrintingRate = BigDecimal(0.001) //印花费率
        val TransferRate = BigDecimal(0.00004)//过户费率
        val CollectionRate = BigDecimal(0.00004) //征管费率
        val RiskRate = BigDecimal(0.00004) //风险金费率



        val FZqbz="GP" //证券标志
        val Fywbz="PT" //业务标志
        val FQsbz="N"  //清算标志
        val FBQTF=BigDecimal(0.00)  //买其他费用
        val FSQTF=BigDecimal(0.00)//卖其他费用
        val FJYFS="PT"
        val Fsh=1
        val FZZR=" "
        val FCHK=" "
        val fzlh="0"
        val ftzbz=" "
        val FBQsghf=BigDecimal(0.00)
        val FsQsghf=BigDecimal(0.00)
        var execution=new /*mutable.ArrayBuffer[ExecutionAggr]()*/ ListBuffer[ExecutionAggrObj]()
        //1）将key进行切分
        val keys=key.split("_")
        val TransactTime = keys(0)//回报时间
        val SecurityID=keys(1)
        val Market=keys(2)
        val ReportingPBUID = keys(3) //回报交易单元
        val Side = keys(4) //买卖方向
        // val AccountID = keys(5) 证券账户
        //2) 如果Side==1,将所有的进行计算，将结果输出到ExecutionAggr中

        if(Side=="1") {

          for (func <- iterable) { //将所有写到ExecutionAggr,最后将类放到可变数组中
            val text = func.split("\t")
            val LastPx = BigDecimal(text(16)) //成交价
            val LastQty = BigDecimal(text(17)) //成交数量
            val LeavesQty = text(18) //订单剩余数量
            //进行计算
            Fbje += LastPx.*(LastQty)
            FBsl += LastQty
            Fbj= LastPx.*(LastQty)
            Fbyhs =BigDecimal(0.0)
            FBjsf += Fbj.*(HandingFeeRate)
            FBzgf += Fbj.*(CollectionRate)
            FBfxj += Fbj.*(RiskRate)

            var FBjs = Fbj.*(HandingFeeRate)
            var FBzg = Fbj.*(CollectionRate)

            Fbyj += Fbj.*(commisionRate) - FBzg - FBjs //买佣金
          }

          Fbsfje = Fbje + FBjsf + FBzgf + FBghf

          val zero=BigDecimal(0).formatted("%.2f")
          val executionAggr=new ExecutionAggrObj(TransactTime,TransactTime,SecurityID,Market,ReportingPBUID,Fbje.setScale(2, RoundingMode.UP).formatted("%.2f"),zero,FBsl.setScale(2, RoundingMode.UP).formatted("%.2f")
            ,zero,Fbyj.setScale(2, RoundingMode.UP).formatted("%.2f"),zero,FBjsf.setScale(2, RoundingMode.UP).formatted("%.2f"),zero,Fbyhs.setScale(2, RoundingMode.UP).formatted("%.2f"),zero,FBzgf.setScale(2, RoundingMode.UP).formatted("%.2f"),zero
            ,FBghf.setScale(2, RoundingMode.UP).formatted("%.2f"),zero,zero,zero,FBfxj.setScale(2, RoundingMode.UP).formatted("%.2f"),zero,Fbsfje.setScale(2, RoundingMode.UP).formatted("%.2f"),zero,FZqbz,Fywbz,FQsbz,FBQTF.formatted("%.2f"),FSQTF.formatted("%.2f"),SecurityID,FJYFS,Fsh,FZZR,FCHK,fzlh,ftzbz,FBQsghf.formatted("%.2f"),FsQsghf.formatted("%.2f"),zero)

          execution.append(executionAggr)

          //3) 如果 Side==2,将所有的进行计算，输出到ExecutionAggr中
        }else if(Side=="2"){

          for (func <- iterable) { //将所有写到ExecutionAggr,最后将类放到可变数组中
            val text = func.split("\t")
            val LastPx = BigDecimal(text(16)) //成交价
            val LastQty = BigDecimal(text(17)) //成交数量
            val LeavesQty = text(18) //订单剩余数量
            //进行计算
            Fsje += LastPx.*(LastQty)  //卖金额
            FSsl += LastQty  //卖数量

            var Fsj =LastPx.*(LastQty) //卖金额

            Fsyhs += Fsj.*(PrintingRate)
            FSjsf += Fsj.*(HandingFeeRate)
            FSzgf += Fsj.*(CollectionRate)
            FSfxj += Fsj.*(RiskRate)

            var FSjs = Fsj.*(HandingFeeRate)
            var FSzg = Fsj.*(CollectionRate)

            Fsyj += Fsj.*(commisionRate) - FSzg - FSjs//卖佣金
          }

          Fsssje += Fsje - FSjsf - FSzgf - FSghf-Fsyhs
          val zero=BigDecimal(0).formatted("%.2f")
          val executionAggr=new ExecutionAggrObj(TransactTime,TransactTime,SecurityID,Market,ReportingPBUID,zero,Fsje.setScale(2, RoundingMode.UP).formatted("%.2f") ,zero
            ,FSsl.setScale(2, RoundingMode.UP).formatted("%.2f"),zero,Fsyj.setScale(2, RoundingMode.UP).formatted("%.2f"),zero,FSjsf.setScale(2, RoundingMode.UP).formatted("%.2f"),zero,Fsyhs.setScale(2, RoundingMode.UP).formatted("%.2f"),zero,FSzgf.setScale(2, RoundingMode.UP).formatted("%.2f")
            ,zero,FSghf.setScale(2, RoundingMode.UP).formatted("%.2f"),zero,zero,FBfxj.setScale(2, RoundingMode.UP)formatted("%.2f"),FSfxj.setScale(2, RoundingMode.UP)formatted("%.2f"),zero,Fsssje.setScale(2, RoundingMode.UP)formatted("%.2f"),FZqbz,Fywbz,FQsbz,FBQTF.formatted("%.2f"),FSQTF.formatted("%.2f"),SecurityID,FJYFS,Fsh,FZZR,FCHK,fzlh,ftzbz,FBQsghf.formatted("%.2f"),FsQsghf.formatted("%.2f"),zero)
          execution.append(executionAggr)
        }
        execution
      }
    }

    /*val dbc = "jdbc:mysql://192.168.102.119:3306/test?user=root&password=root1234" /*JJCWGZ*/
     classOf[com.mysql.jdbc.Driver]
     val conn = DriverManager.getConnection(dbc)


     val driver = "com.mysql.jdbc.Driver"
     val url = "jdbc:mysql://192.168.102.119:3306/test"
     val username = "root"
     val password = "root1234"

     // do database insert
     try {
       Class.forName(driver)
       val connection = DriverManager.getConnection(url, username, password)


       val prep = conn.prepareStatement("create table SJSV5(" +
         "  `FDATE`   VARCHAR(30) not null,  `FINDATE` VARCHAR(30)  not null," +
         "  `FZQDM`   VARCHAR(10) not null,  `FSZSH`   VARCHAR(1) not null," +
         "  `FJYXWH`  VARCHAR(10) not null,  `FBJE`    decimal(18,2) not null," +
         "  `FSJE`   decimal(18,2) not null,  `FBSL`   decimal(18,2) not null," +
         "  `FSSL`   decimal(18,2) not null,  `FBYJ`   decimal(18,2) not null," +
         "  `FSYJ`   decimal(18,2) not null,  `FBJSF`  decimal(18,2) not null," +
         "  `FSJSF`  decimal(18,2) not null,  `FBYHS`  decimal(18,2) not null," +
         "  `FSYHS`  decimal(18,2) not null, `FBZGF`  decimal(18,2) not null," +
         "  `FSZGF`   decimal(18,2) not null,  `FBGHF`  decimal(18,2) not null, " +
         " `FSGHF`  decimal(18,2) not null, `FBGZLX` decimal(18,2) not null," +
         "  `FSGZLX` decimal(18,2) not null,  `FBFXJ`  decimal(18,2) not null," +
         " `FSFXJ`  decimal(18,2) not null,  `FBSFJE` decimal(18,2) not null," +
         "  `FSSSJE` decimal(18,2) not null,  `FZQBZ`   VARCHAR(20) not null," +
         "  `FYWBZ`   VARCHAR(20) not null,  `FQSBZ`   VARCHAR(1) not null," +
         "  `FBQTF`  decimal(18,2) not null,  `FSQTF`  decimal(18,2) not null," +
         "  `ZQDM`    VARCHAR(10) not null,  `FJYFS`   VARCHAR(10) default 'PT' not null," +
         "  `FSH`     INTEGER default 1,  `FZZR`    VARCHAR(20) default ' ' not null," +
         "  `FCHK`    VARCHAR(20) default ' ',  `FZLH`    VARCHAR(30) default '0' not null," +
         "  `FTZBZ`   VARCHAR(1) default ' ' not null, `FBQSGHF` decimal(18,2) default 0," +
         "  `FSQSGHF` decimal(18,2) default 0,  `FGDDM`   VARCHAR(18) default ' ' not null) ")
     }
     finally {
       conn.close
     }*/

    import sparkSession.implicits._
    finallData.toDF().write.format("jdbc")
      .option("url","jdbc:mysql://192.168.102.119:3306/JJCWGZ")
      .option("user","root")
      .option("password","root1234")
      .option("dbtable","SJSV5")
      .mode(SaveMode.Append)
      .save()
    /*import sparkSession.implicits._
      finallData.toDF().show()*/
  }





}
