package com.yss.scala.guzhi

import java.text.SimpleDateFormat
import java.util.{Date, Locale, Properties}

import com.yss.scala.util.Util
import org.apache.commons.lang.time.FastDateFormat
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.math.BigDecimal.RoundingMode

/**
  * @author MingZhang Wang
  * @version 2018-09-17 17:25
  *          describe:
  *          目标文件：
  *          目标表：
  */
object ShenzhenStockExchangeTriPartyRepo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SHDZGH").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "root1234")
    val sjsmxDF: DataFrame = spark.read.jdbc("jdbc:mysql://localhost:3306/test3?useUnicode=true&characterEncoding=utf8", "SJSSFHGMX", properties)
    val sjsjgDF: DataFrame = spark.read.jdbc("jdbc:mysql://localhost:3306/test3?useUnicode=true&characterEncoding=utf8", "SJSSFHGJG", properties)

    sjsmxDF.createOrReplaceTempView("MXTemp")
    val xjxmxTempDF: DataFrame = spark.sqlContext.sql("select MXFJSM,MXYWLB,FJETemp,MXCJRQ from MXTemp")

    sjsjgDF.createOrReplaceTempView("JGTemp")
    val xjxjgTempDF: DataFrame = spark.sqlContext.sql("select JGFJSM,JGYWLB,FJETemp,JGCJRQ from JGTemp")


    // 读取费率表
    val csyjlv: RDD[String] = sc.textFile("C:/Users/hasee/Desktop/work/part-m-00000")
    // 将费率表转换成DateFrame
    val schemaString: String = "FID FZQLB FSZSH FLV FLVMIN FTJ1 FSTR1 FTJ2 FTJ2FROM FTJ2TO FLVZK FSH FZZR FCHK FSTARTDATE FJJDM FGDJE"
    val fields: Array[StructField] = schemaString.split(" ").map(fieldname => StructField(fieldname, StringType, nullable = true))
    val schema = StructType(fields)
    val rowRDD: RDD[Row] = csyjlv.map(_.split(",")).map(attributes => Row(attributes(0).trim, attributes(1).trim, attributes(2).trim, attributes(3).trim, attributes(4).trim, attributes(5).trim, attributes(6).trim, attributes(7).trim, attributes(8).trim, attributes(9).trim, attributes(10).trim, attributes(11).trim, attributes(12).trim, attributes(13).trim, attributes(14).trim, attributes(15).trim, attributes(16).trim))
    val csyjlvDF: DataFrame = spark.createDataFrame(rowRDD, schema)

    //生成临时表并筛选数据
    csyjlvDF.createOrReplaceTempView("csyjlvTemp1")
    val csyjlvTableDF: DataFrame = spark.sqlContext.sql("select FZQLB,FSZSH,FSTR1,FLV from csyjlvTemp1")
    // 广播出去
    val csyjlvTableDFBroadCast: Broadcast[DataFrame] = sc.broadcast(csyjlvTableDF)

    //明细-非到期续作前期合约了结数据取值规则
    val sjsmxXzxkRDD = sjsmxDF.rdd.map(row => {

      val mxywlb: String = row.getAs[String]("") // 明细业务类别
      val FDate: String = row.getAs[String]("MXCJRQ") //日期
      val FJyxwh: String = row.getAs[String]("MXJYDY") //交易席位号
      val FHTXH: String = row.getAs[String]("MXYWLSH") //合同序号
      val FCSHTXH: String = row.getAs[String]("MXFJSM") //初始合同序号

      val Fjsf: BigDecimal = BigDecimal(row.getAs[String]("MXJYJSF")).abs //经手费

      val FZqdm: String = " " //证券代码
      val FSzsh: String = "S" //交易市场
      val FZqbz: String = "ZQ" //证券表示
      val Fsh: Int = 1 //审核
      val Fzzr: String = "admin" //制作人:当前用户
      val Fchk: String = "admin" //审核人:当前用户
      val FSJLY: String = "ZD" //数据来源


      var Fjybz: String = new String //交易标志
      var FJyFs: String = new String //交易方式
      var FinDate: String = new String //日期
      var Fje: BigDecimal = BigDecimal(0) //成交金额
      var Fyj: BigDecimal = BigDecimal(0) //佣金
      var FHggain: BigDecimal = BigDecimal(0)
      //回购收益
      var FSSSFJE: BigDecimal = BigDecimal(0)
      //实收实付金额
      var FRZLV: BigDecimal = BigDecimal(0) //融资/回购利率
      var FCSGHQX: BigDecimal = BigDecimal(0) //初始购回期限


      val FSETCODE = BigDecimal(0) // 套账号
      val FSL = BigDecimal(0) // 成交数量
      val Fyhs = BigDecimal(0) // 印花税
      val Fzgf = BigDecimal(0) //征管费
      val Fghf = BigDecimal(0) //过户费
      val FFxj = BigDecimal(0) // 风险金
      val FQtf = BigDecimal(0) // 其他费用
      val Fgzlx = BigDecimal(0) //国债利息
      val FQsbz = "" //清算标志
      val Ftzbz = "" //投资标志
      val FQsghf = "" //券商过户费
      val FGddm = "" //股东代码
      val fzlh = "" //指令号
      val ISRTGS = "" //结算方式
      val FPARTID = "" // 结算会员
      val FYwbz = "" // 业务标志
      val Fbz = "" // 币种
      val ZqDm = "" //证券代码
      val FBS = "" // 买卖方向

      //      var FZQLB: String = new String //证券类别
      //      var FSZSH: String = new String //席位地点
      //      var FSTR1: String = new String //席位号
      //      var FLV: String = new String //佣金利率


      //交易标志判断规则
      mxywlb match {
        case "SFCS" => Fjybz = "CS_SFHG"
        case "SFDQ" => Fjybz = "DQ_SFHG"
        case "SFXZ" => Fjybz = "XZXK_SFHG"
        case "SFTG" => Fjybz = "TQGH_SFHG"
        case "SFJZ" => Fjybz = "SFJZ"
      }

      //交易方式判断规则
      val MXQSBJ: BigDecimal = BigDecimal(row.getAs[String]("MXQSBJ"))
      if ("SFCS".equals(mxywlb)) {
        if (MXQSBJ > 0) {
          FJyFs = row.getAs[String]("RZ")
        } else if (MXQSBJ < 0) {
          FJyFs = row.getAs[String]("CZ")
        }
      } else if ("SFDQ SFTG SFJZ".equals(mxywlb)) {
        if (MXQSBJ <= 0) {
          FJyFs = row.getAs[String]("RZ")
        } else if (MXQSBJ >= 0) {
          FJyFs = row.getAs[String]("CZ")
        }
      } else if ("SFXZ".equals(mxywlb)) {
        if (MXQSBJ >= 0) {
          FJyFs = row.getAs[String]("RZ")
        } else if (MXQSBJ <= 0) {
          FJyFs = row.getAs[String]("CZ")
        }
      }

      // 实收实付金额判断规则
      if ("SFCS".equals(mxywlb) || "SDFQ".equals(mxywlb) || "SFTG".equals(mxywlb) || "SFJZ".equals(mxywlb)) {
        FSSSFJE = BigDecimal(row.getAs[String]("MXSFJE"))
      } else if ("SFXZ".equals(mxywlb)) {
        FSSSFJE = MXQSBJ.abs - Fjsf
      }

      //日期、成交金额、佣金、回购收益、初始购回期限、融资利率判断规则
      if ("SFCS".equals(mxywlb) || "SFXZ".equals(mxywlb)) {

        val FLVRDD = csyjlvTableDFBroadCast.value.rdd.filter(row => {
          val FZQLB: String = row.getAs[String]("FZQLB") //证券类别
          val FSZSH: String = row.getAs[String]("FSZSH") //席位地点
          val FSTR1: String = row.getAs[String]("FSTR1") //席位号
          "ZQZYSFHG".equals(FZQLB) && "S".equals(FSZSH) && FJyxwh.equals(FSTR1)
        })
        val FLV: BigDecimal = BigDecimal(FLVRDD.toString()) //佣金费率

        FinDate = row.getAs[String]("MXQTRQ") //日期
        Fje = MXQSBJ.abs // 成交金额
        Fyj = Fje.*(FLV).setScale(2, RoundingMode.HALF_UP) // 佣金
        FRZLV = BigDecimal(row.getAs[String]("MXCJJG")) / 100 //融资/回购利率
        FHggain = MXQSBJ - Fje //回购收益

        //        val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        //        val endDate = simpleDateFormat.parse(FinDate)
        //        val startDate = simpleDateFormat.parse(row.getAs[String]("MXCJRQ"))
        //
        //        FCSGHQX = (endDate - startDate)/(24*60*60*1000) //初试购回期限

        FCSGHQX = row.getAs[BigDecimal]("FCSGHQXTemp") //初始购回期限

      } else if ("SFDQ".equals(mxywlb) || "SFTG".equals(mxywlb) || "SFJZ".equals(mxywlb)) {
        FinDate = row.getAs[String]("MXCJRQ") //购回日期

        //获取对应初试合同序号的业务类别为SFXZ的回购业务
        val FjeRDD = xjxmxTempDF.rdd.filter(row => {
          val MXFJSM: String = row.getAs[String]("MXFJSM")
          val MXYWLB: String = row.getAs[String]("MXYWLB")
          val FJETemp: String = row.getAs[String]("FJETemp")

          FCSHTXH.equals(MXFJSM) && "SFXZ".equals(MXYWLB) || "SFCS".equals(MXYWLB)
        })

        val FjeArray: Array[String] = FjeRDD.toString.split(",")
        Fje = BigDecimal(FjeArray(2)) // 成交/购回金额
        Fyj = BigDecimal(0) // 佣金
        FHggain = MXQSBJ - Fje //回购收益

        val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

        val strYear1=FjeArray(3).substring(0,4)
        val strMonth1 = FjeArray(3).substring(4,6)
        val strDay1 = FjeArray(3).substring(6,8)
        val FJEDate = strYear1 +"-"+ strMonth1 + "-" + strDay1

        val strYear2=FinDate.substring(0,4)
        val strMonth2 =FinDate.substring(4,6)
        val strDay2 = FinDate.substring(6,8)
        val FinDateDate = strYear2 +"-"+ strMonth2 + "-" + strDay2

        val startDate = simpleDateFormat.parse(FJEDate)
        val endDate = simpleDateFormat.parse(FinDateDate)
        FCSGHQX = (endDate.getTime - startDate.getTime) / (24 * 60 * 60 * 1000) //初始购回期限
        FRZLV = BigDecimal(0) //融资/回购利率
      }

      SJSSFHGYW(
        FDate,
        FinDate,
        FZqdm,
        FSzsh,
        FJyxwh,
        Fje,
        Fyj,
        Fjsf,
        FHggain,
        FSSSFJE,
        FZqbz,
        Fjybz,
        FJyFs,
        Fsh,
        Fzzr,
        Fchk,
        FHTXH,
        FSETCODE,
        FCSGHQX,
        FRZLV,
        FSJLY,
        FCSHTXH,
        FBS,
        FSL,
        Fyhs,
        Fzgf,
        Fghf,
        FFxj,
        FQtf,
        Fgzlx,
        FQsbz,
        Ftzbz,
        FQsghf,
        FGddm,
        fzlh,
        ISRTGS,
        FPARTID,
        FYwbz,
        Fbz,
        ZqDm
      )
    })

    import spark.implicits._
    Util.outputMySql(sjsmxXzxkRDD.toDF(), "")

    //明细-到期续作前期合约了结数据取值规则
    val sjsmxFilterRDD = sjsmxDF.rdd.filter(row => {
      val MXYWLB: String = row.getAs[String]("MXYWLB")
      "SFXZ".equals(MXYWLB)
    })

    val sjsmxXzljRDD = sjsmxFilterRDD.map(row => {

      val FDate: String = row.getAs[String]("MXCJRQ") //成交日期
      val FinDate: String = row.getAs[String]("MXCJRQ")
      // 初始购回日期
      val FJyxwh: String = row.getAs[String]("MXJYDY") //交易席位号
      val Fsssfje: BigDecimal = BigDecimal(row.getAs[String]("MXZJJE")).abs //实收实付金额
      val FHTXH: String = row.getAs[String]("MXYWLSH") //合同序号
      val FCSHTXH: String = row.getAs[String]("MXFJSM") //初始合同序号


      val FZqdm: String = " " //证券代码
      val FSzsh: String = "S" //交易市场
      val Fyj: BigDecimal = BigDecimal(0) //佣金
      val Fjsf: BigDecimal = BigDecimal(0) //经手费
      val FZqbz: String = "ZQ" //证券表示
      val Fjybz: String = "XZLJ_SFHG" //交易标志
      val Fsh: Int = 1 //审核
      val Fzzr: String = "admin" //制作人
      val Fchk: String = "admin" //审核人
      val FSJLY: String = "ZD" //数据来源


      val FSETCODE = BigDecimal(0) // 套账号
      val FSL = BigDecimal(0) // 成交数量
      val Fyhs = BigDecimal(0) // 印花税
      val Fzgf = BigDecimal(0) //征管费
      val Fghf = BigDecimal(0) //过户费
      val FFxj = BigDecimal(0) // 风险金
      val FQtf = BigDecimal(0) // 其他费用
      val Fgzlx = BigDecimal(0) //国债利息
      val FQsbz = "" //清算标志
      val Ftzbz = "" //投资标志
      val FQsghf = "" //券商过户费
      val FGddm = "" //股东代码
      val fzlh = "" //指令号
      val ISRTGS = "" //结算方式
      val FPARTID = "" // 结算会员
      val FYwbz = "" // 业务标志
      val Fbz = "" // 币种
      val ZqDm = "" //证券代码
      val FBS = "" // 买卖方向


      val sjsmxSFCSRDD = sjsmxDF.rdd.filter(row => {
        "SFCS".equals(row.getAs[String]("MXYWLB")) && FCSHTXH.equals(row.getAs[String]("MXFJSM"))
      })
      val SFCSvalue = sjsmxSFCSRDD.map(row => {
        val Fje = row.getAs[BigDecimal]("FJETemp")
        val FHggain = row.getAs[BigDecimal]("FHggain")
        val FRZLV = row.getAs[BigDecimal]("FRZLV")

        Fje + "_" + FHggain + "_" + FRZLV
      })
      val sjsmxSFCS: Array[String] = SFCSvalue.toString().split("_")

      val Fje: BigDecimal = BigDecimal(sjsmxSFCS(0)) //成交金额
      val FHggain: BigDecimal = BigDecimal(sjsmxSFCS(1)) //回购收益
      val FRZLV: BigDecimal = BigDecimal(sjsmxSFCS(2)) //融资/购回利率

      val FCSGHQX = row.getAs[BigDecimal]("FCSGHQXTemp") //初始购回期限

      var FJyFs: String = new String

      if (BigDecimal(row.getAs[String]("MXQSBJ")) < 0) {
        FJyFs = "RZ" //交易方式
      } else if (BigDecimal(row.getAs[String]("MXQSBJ")) > 0) {
        FJyFs = "CZ" //交易方式
      }

      SJSSFHGYW(
        FDate,
        FinDate,
        FZqdm,
        FSzsh,
        FJyxwh,
        Fje,
        Fyj,
        Fjsf,
        FHggain,
        Fsssfje,
        FZqbz,
        Fjybz,
        FJyFs,
        Fsh,
        Fzzr,
        Fchk,
        FHTXH,
        FSETCODE,
        FCSGHQX,
        FRZLV,
        FSJLY,
        FCSHTXH,
        FBS,
        FSL,
        Fyhs,
        Fzgf,
        Fghf,
        FFxj,
        FQtf,
        Fgzlx,
        FQsbz,
        Ftzbz,
        FQsghf,
        FGddm,
        fzlh,
        ISRTGS,
        FPARTID,
        FYwbz,
        Fbz,
        ZqDm
      )
    })
    import spark.implicits._
    Util.outputMySql(sjsmxXzljRDD.toDF(), "")

    //结果-非到期续作前期合约了结数据取值规则
    val sjsjgXzxkRDD = sjsjgDF.rdd.map(row => {

      val jgywlb: String = row.getAs[String]("") // 明细业务类别
      val FDate: String = row.getAs[String]("JGCJRQ") //日期
      val FJyxwh: String = row.getAs[String]("JGJYDY") //交易席位号
      val FHTXH: String = row.getAs[String]("JGYWLSH") //合同序号
      val FCSHTXH: String = row.getAs[String]("JGFJSM") //初始合同序号

      val Fjsf: BigDecimal = BigDecimal(row.getAs[String]("JGJYJSF")).abs //经手费

      val FZqdm: String = " " //证券代码
      val FSzsh: String = "S" //交易市场
      val FZqbz: String = "ZQ" //证券表示
      val Fsh: Int = 1 //审核
      val Fzzr: String = "admin" //制作人:当前用户
      val Fchk: String = "admin" //审核人:当前用户
      val FSJLY: String = "ZD" //数据来源


      var Fjybz: String = new String //交易标志
      var FJyFs: String = new String //交易方式
      var FinDate: String = new String //日期
      var Fje: BigDecimal = BigDecimal(0) //成交金额
      var Fyj: BigDecimal = BigDecimal(0) //佣金
      var FHggain: BigDecimal = BigDecimal(0)
      //回购收益
      var FSSSFJE: BigDecimal = BigDecimal(0)
      //实收实付金额
      var FRZLV: BigDecimal = BigDecimal(0) //融资/回购利率
      var FCSGHQX: BigDecimal = BigDecimal(0) //初始购回期限


      val FSETCODE = BigDecimal(0) // 套账号
      val FSL = BigDecimal(0) // 成交数量
      val Fyhs = BigDecimal(0) // 印花税
      val Fzgf = BigDecimal(0) //征管费
      val Fghf = BigDecimal(0) //过户费
      val FFxj = BigDecimal(0) // 风险金
      val FQtf = BigDecimal(0) // 其他费用
      val Fgzlx = BigDecimal(0) //国债利息
      val FQsbz = "" //清算标志
      val Ftzbz = "" //投资标志
      val FQsghf = "" //券商过户费
      val FGddm = "" //股东代码
      val fzlh = "" //指令号
      val ISRTGS = "" //结算方式
      val FPARTID = "" // 结算会员
      val FYwbz = "" // 业务标志
      val Fbz = "" // 币种
      val ZqDm = "" //证券代码
      val FBS = "" // 买卖方向

      //      var FZQLB: String = new String //证券类别
      //      var FSZSH: String = new String //席位地点
      //      var FSTR1: String = new String //席位号
      //      var FLV: String = new String //佣金利率


      //交易标志判断规则
      jgywlb match {
        case "SFCS" => Fjybz = "CS_SFHG"
        case "SFDQ" => Fjybz = "DQ_SFHG"
        case "SFXZ" => Fjybz = "XZXK_SFHG"
        case "SFTG" => Fjybz = "TQGH_SFHG"
        case "SFJZ" => Fjybz = "SFJZ"
      }

      //交易方式判断规则
      val JGQSBJ: BigDecimal = BigDecimal(row.getAs[String]("MXQSBJ"))
      if ("SFCS".equals(jgywlb)) {
        if (JGQSBJ > 0) {
          FJyFs = row.getAs[String]("RZ")
        } else if (JGQSBJ < 0) {
          FJyFs = row.getAs[String]("CZ")
        }
      } else if ("SFDQ SFTG SFJZ".equals(jgywlb)) {
        if (JGQSBJ <= 0) {
          FJyFs = row.getAs[String]("RZ")
        } else if (JGQSBJ >= 0) {
          FJyFs = row.getAs[String]("CZ")
        }
      } else if ("SFXZ".equals(jgywlb)) {
        if (JGQSBJ >= 0) {
          FJyFs = row.getAs[String]("RZ")
        } else if (JGQSBJ <= 0) {
          FJyFs = row.getAs[String]("CZ")
        }
      }

      // 实收实付金额判断规则
      if ("SFCS".equals(jgywlb) || "SDFQ".equals(jgywlb) || "SFTG".equals(jgywlb) || "SFJZ".equals(jgywlb)) {
        FSSSFJE = BigDecimal(row.getAs[String]("MXSFJE"))
      } else if ("SFXZ".equals(jgywlb)) {
        FSSSFJE = JGQSBJ.abs - Fjsf
      }

      //日期、成交金额、佣金、回购收益、初始购回期限、融资利率判断规则
      if ("SFCS".equals(jgywlb) || "SFXZ".equals(jgywlb)) {

        val FLVRDD = csyjlvTableDFBroadCast.value.rdd.filter(row => {
          val FZQLB: String = row.getAs[String]("FZQLB") //证券类别
          val FSZSH: String = row.getAs[String]("FSZSH") //席位地点
          val FSTR1: String = row.getAs[String]("FSTR1") //席位号
          "ZQZYSFHG".equals(FZQLB) && "S".equals(FSZSH) && FJyxwh.equals(FSTR1)
        })
        val FLV: BigDecimal = BigDecimal(FLVRDD.toString()) //佣金费率

        FinDate = row.getAs[String]("MXQTRQ") //日期
        Fje = JGQSBJ.abs // 成交金额
        Fyj = Fje.*(FLV).setScale(2, RoundingMode.HALF_UP) // 佣金
        FRZLV = BigDecimal(row.getAs[String]("MXCJJG")) / 100 //融资/回购利率
        FHggain = JGQSBJ - Fje //回购收益

        //        val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        //        val endDate = simpleDateFormat.parse(FinDate)
        //        val startDate = simpleDateFormat.parse(row.getAs[String]("MXCJRQ"))
        //
        //        FCSGHQX = (endDate - startDate)/(24*60*60*1000) //初试购回期限

        FCSGHQX = row.getAs[BigDecimal]("FCSGHQXTemp") //初始购回期限

      } else if ("SFDQ".equals(jgywlb) || "SFTG".equals(jgywlb) || "SFJZ".equals(jgywlb)) {
        FinDate = row.getAs[String]("MXCJRQ") //购回日期

        //获取对应初试合同序号的业务类别为SFXZ的回购业务
        val FjeRDD = xjxjgTempDF.rdd.filter(row => {
          val JGFJSM: String = row.getAs[String]("JGFJSM")
          val JGYWLB: String = row.getAs[String]("JGYWLB")
          val FJETemp: String = row.getAs[String]("FJETemp")

          FCSHTXH.equals(JGFJSM) && "SFXZ".equals(JGYWLB) || "SFCS".equals(JGYWLB)
        })

        val FjeArray: Array[String] = FjeRDD.toString.split(",")
        Fje = BigDecimal(FjeArray(2)) // 成交/购回金额
        Fyj = BigDecimal(0) // 佣金
        FHggain = JGQSBJ - Fje //回购收益

        val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

        val strYear1=FjeArray(3).substring(0,4)
        val strMonth1 = FjeArray(3).substring(4,6)
        val strDay1 = FjeArray(3).substring(6,8)
        val FJEDate = strYear1 +"-"+ strMonth1 + "-" + strDay1

        val strYear2=FinDate.substring(0,4)
        val strMonth2 =FinDate.substring(4,6)
        val strDay2 = FinDate.substring(6,8)
        val FinDateDate = strYear2 +"-"+ strMonth2 + "-" + strDay2

        val startDate = simpleDateFormat.parse(FJEDate)
        val endDate = simpleDateFormat.parse(FinDateDate)
        FCSGHQX = (endDate.getTime - startDate.getTime) / (24 * 60 * 60 * 1000) //初始购回期限


        FRZLV = BigDecimal(0) //融资/回购利率
      }

      SJSSFHGYW(
        FDate,
        FinDate,
        FZqdm,
        FSzsh,
        FJyxwh,
        Fje,
        Fyj,
        Fjsf,
        FHggain,
        FSSSFJE,
        FZqbz,
        Fjybz,
        FJyFs,
        Fsh,
        Fzzr,
        Fchk,
        FHTXH,
        FSETCODE,
        FCSGHQX,
        FRZLV,
        FSJLY,
        FCSHTXH,
        FBS,
        FSL,
        Fyhs,
        Fzgf,
        Fghf,
        FFxj,
        FQtf,
        Fgzlx,
        FQsbz,
        Ftzbz,
        FQsghf,
        FGddm,
        fzlh,
        ISRTGS,
        FPARTID,
        FYwbz,
        Fbz,
        ZqDm
      )
    })

    import spark.implicits._
    Util.outputMySql(sjsjgXzxkRDD.toDF(), "")

    //结果-到期续作前期合约了结数据取值规则
    val sjsjgFilterRDD = sjsjgDF.rdd.filter(row => {
      val JGYWLB: String = row.getAs[String]("JGYWLB")
      "SFXZ".equals(JGYWLB)
    })

    val sjsjgXzljRDD = sjsjgFilterRDD.map(row => {

      val FDate: String = row.getAs[String]("JGCJRQ") //成交日期
      val FinDate: String = row.getAs[String]("JGCJRQ")
      // 初始购回日期
      val FJyxwh: String = row.getAs[String]("JGJYDY") //交易席位号
      val Fsssfje: BigDecimal = BigDecimal(row.getAs[String]("JGZJJE")).abs //实收实付金额
      val FHTXH: String = row.getAs[String]("JGYWLSH") //合同序号
      val FCSHTXH: String = row.getAs[String]("JGFJSM") //初始合同序号


      val FZqdm: String = " " //证券代码
      val FSzsh: String = "S" //交易市场
      val Fyj: BigDecimal = BigDecimal(0) //佣金
      val Fjsf: BigDecimal = BigDecimal(0) //经手费
      val FZqbz: String = "ZQ" //证券表示
      val Fjybz: String = "XZLJ_SFHG" //交易标志
      val Fsh: Int = 1 //审核
      val Fzzr: String = "admin" //制作人
      val Fchk: String = "admin" //审核人
      val FSJLY: String = "ZD" //数据来源


      val FSETCODE = BigDecimal(0) // 套账号
      val FSL = BigDecimal(0) // 成交数量
      val Fyhs = BigDecimal(0) // 印花税
      val Fzgf = BigDecimal(0) //征管费
      val Fghf = BigDecimal(0) //过户费
      val FFxj = BigDecimal(0) // 风险金
      val FQtf = BigDecimal(0) // 其他费用
      val Fgzlx = BigDecimal(0) //国债利息
      val FQsbz = "" //清算标志
      val Ftzbz = "" //投资标志
      val FQsghf = "" //券商过户费
      val FGddm = "" //股东代码
      val fzlh = "" //指令号
      val ISRTGS = "" //结算方式
      val FPARTID = "" // 结算会员
      val FYwbz = "" // 业务标志
      val Fbz = "" // 币种
      val ZqDm = "" //证券代码
      val FBS = "" // 买卖方向


      val sjsjgSFCSRDD = sjsjgDF.rdd.filter(row => {
        "SFCS".equals(row.getAs[String]("JGYWLB")) && FCSHTXH.equals(row.getAs[String]("JGFJSM"))
      })
      val SFCSvalue = sjsjgSFCSRDD.map(row => {
        val Fje = row.getAs[BigDecimal]("FJETemp")
        val FHggain = row.getAs[BigDecimal]("FHggain")
        val FRZLV = row.getAs[BigDecimal]("FRZLV")

        Fje + "_" + FHggain + "_" + FRZLV
      })
      val sjsjgSFCS: Array[String] = SFCSvalue.toString().split("_")

      val Fje: BigDecimal = BigDecimal(sjsjgSFCS(0)) //成交金额
      val FHggain: BigDecimal = BigDecimal(sjsjgSFCS(1)) //回购收益
      val FRZLV: BigDecimal = BigDecimal(sjsjgSFCS(2)) //融资/购回利率

      val FCSGHQX = row.getAs[BigDecimal]("FCSGHQXTemp") //初始购回期限

      var FJyFs: String = new String

      if (BigDecimal(row.getAs[String]("JGQSBJ")) < 0) {
        FJyFs = "RZ" //交易方式
      } else if (BigDecimal(row.getAs[String]("JGQSBJ")) > 0) {
        FJyFs = "CZ" //交易方式
      }

      SJSSFHGYW(
        FDate,
        FinDate,
        FZqdm,
        FSzsh,
        FJyxwh,
        Fje,
        Fyj,
        Fjsf,
        FHggain,
        Fsssfje,
        FZqbz,
        Fjybz,
        FJyFs,
        Fsh,
        Fzzr,
        Fchk,
        FHTXH,
        FSETCODE,
        FCSGHQX,
        FRZLV,
        FSJLY,
        FCSHTXH,
        FBS,
        FSL,
        Fyhs,
        Fzgf,
        Fghf,
        FFxj,
        FQtf,
        Fgzlx,
        FQsbz,
        Ftzbz,
        FQsghf,
        FGddm,
        fzlh,
        ISRTGS,
        FPARTID,
        FYwbz,
        Fbz,
        ZqDm
      )
    })
    import spark.implicits._
    Util.outputMySql(sjsjgXzljRDD.toDF(), "")

  }
}
case class SJSSFHGYW(
                      FDate: String, //日期
                      FinDate: String, //日期
                      FZqdm: String, //证券代码
                      FSzsh: String, //交易市场
                      FJyxwh: String, //交易席位号
                      Fje: BigDecimal, //成交金额
                      Fyj: BigDecimal, //佣金
                      Fjsf: BigDecimal, //经手费
                      FHggain: BigDecimal, //回购收益
                      FSSSFJE: BigDecimal, // 实收实付金额
                      FZqbz: String, //证券标识
                      Fjybz: String, //交易标识
                      Fjyfs: String, //交易方式
                      Fsh: Int, //审核
                      Fzzr: String, // 制作人
                      Fchk: String, // 审核人
                      FHTXH: String, //合同序号
                      FSETCODE: BigDecimal, //套账号
                      FCSGHQX: BigDecimal, //初试购回期限
                      FRZLV: BigDecimal, //融资(回购利率)
                      FSJLY: String, // 数据来源
                      FCSHTXH: String, //合同序号
                      FBS: String,
                      FSL: BigDecimal,
                      Fyhs: BigDecimal,
                      Fzgf: BigDecimal,
                      Fghf: BigDecimal,
                      FFxj: BigDecimal,
                      FQtf: BigDecimal,
                      Fgzlx: BigDecimal,
                      FQsbz: String,
                      Ftzbz: String,
                      FQsghf: String,
                      FGddm: String,
                      Fzlh: String,
                      ISRTGS: String,
                      FPARTID: String,
                      FYwbz: String,
                      Fbz: String,
                      ZqDm: String
                    )