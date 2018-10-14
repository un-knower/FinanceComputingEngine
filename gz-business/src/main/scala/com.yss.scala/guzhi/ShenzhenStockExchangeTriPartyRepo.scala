package com.yss.scala.guzhi

import java.text.SimpleDateFormat
import java.util.Properties

import com.yss.scala.dto.ShenzhenStockExchangeTriPartyRepoDto
import com.yss.scala.util.{DateUtils, Util}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

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
    val sjsmxDF: DataFrame = spark.read.jdbc("jdbc:mysql://192.168.102.120:3306/JJCWGZ", "sjsmxETL_wmz", properties)
    val sjsjgDF: DataFrame = spark.read.jdbc("jdbc:mysql://192.168.102.120:3306/JJCWGZ", "sjsjgETL_wmz", properties)

    val sjsmxDFArr: Array[Row] = sjsmxDF.collect()

    val sjsmxDFArrBroadCast = sc.broadcast(sjsmxDFArr)

    val sjsjgDFArr = sjsjgDF.collect()

    val sjsjgDFArrBroadCast: Broadcast[Array[Row]] = sc.broadcast(sjsjgDFArr)






    sjsmxDF.createOrReplaceTempView("MXTemp")
    val sjsmxTempDF: DataFrame = spark.sqlContext.sql("select MXFJSM,MXYWLB,FJETemp,MXCJRQ from MXTemp")
    val sjsmxTempBrodcast: Broadcast[DataFrame] = sc.broadcast(sjsmxTempDF)

    sjsjgDF.createOrReplaceTempView("JGTemp")
    val sjsjgTempDF: DataFrame = spark.sqlContext.sql("select JGFJSM,JGYWLB,FJETemp,JGCJRQ from JGTemp")
    val sjsjgTempBroadcast = sc.broadcast(sjsjgTempDF)

    val day = DateUtils.formatDate(System.currentTimeMillis())
    // 读取费率表
    val csyjlv: RDD[String] = sc.textFile("hdfs://192.168.102.120:8020/yss/guzhi/basic_list/" + day + "/A001CSYJLV/part-m-00000")
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


    var mxywlb: String = ""
    var FDate: String = ""
    var FJyxwh: String = ""
    var FHTXH: String = ""
    var FCSHTXH: String = ""
    var Fjsf: BigDecimal = BigDecimal(0)
    var Flv: BigDecimal = BigDecimal(0)


    var FZQLB: String = ""
    var FSZSH: String = ""
    var FSTR1: String = ""
    var MXFJSM: String = ""
    var MXYWLB: String = ""
    var FJETemp: String = ""
    var FCSGHQX: BigDecimal = BigDecimal(0)
    var FinDate: String = ""
    var FSSSFJE: BigDecimal = BigDecimal(0)

    var JGFJSM: String = ""
    var JGYWLB: String = ""

    var Fje: BigDecimal = BigDecimal(0)
    var FHggain: BigDecimal = BigDecimal(0)
    var FRZLV: BigDecimal = BigDecimal(0)
    var MXQSBJ: BigDecimal = BigDecimal(0)

    //    val rdd = sjsmxDF.rdd.count()
    //明细-非到期续作前期合约了结数据取值规则
    val sjsmxXzxkRDD = sjsmxDF.rdd.map(row => {

      mxywlb = row.getAs[String]("MXYWLB") // 明细业务类别
      FDate = row.getAs[String]("MXCJRQ") //日期
      FJyxwh = row.getAs[String]("MXJYDY") //交易席位号
      FHTXH = row.getAs[String]("MXYWLSH") //合同序号
      FCSHTXH = row.getAs[String]("MXFJSM") //初始合同序号
      MXQSBJ = BigDecimal(row.getAs[String]("MXQSBJ"))
      Fjsf = BigDecimal(row.getAs[String]("MXJYJSF")).abs //经手费

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
      //      var Fje: BigDecimal = BigDecimal(0) //成交金额
      var Fyj: BigDecimal = BigDecimal(0) //佣金
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

      //交易标志判断规则
      mxywlb match {
        case "SFCS" => Fjybz = "CS_SFHG"
        case "SFDQ" => Fjybz = "DQ_SFHG"
        case "SFXZ" => Fjybz = "XZXK_SFHG"
        case "SFTG" => Fjybz = "TQGH_SFHG"
        case "SFJZ" => Fjybz = "SFJZ"
      }

      //交易方式判断规则

      if ("SFCS".equals(mxywlb)) {
        if (MXQSBJ > 0) {
          FJyFs = "RZ"
        } else if (MXQSBJ < 0) {
          FJyFs = "CZ"
        }
      } else if ("SFDQ SFTG SFJZ".contains(mxywlb)) {
        if (MXQSBJ < 0) {
          FJyFs = "RZ"
        } else if (MXQSBJ > 0) {
          FJyFs = "CZ"
        }
      } else if ("SFXZ".equals(mxywlb)) {
        if (MXQSBJ > 0) {
          FJyFs = "RZ"
        } else if (MXQSBJ < 0) {
          FJyFs = "CZ"
        }
      }

      // 实收实付金额判断规则
      if ("SFCS".equals(mxywlb) || "SFDQ".equals(mxywlb) || "SFTG".equals(mxywlb) || "SFJZ".equals(mxywlb)) {
        FSSSFJE = BigDecimal(row.getAs[String]("MXSFJE")).abs
      } else if ("SFXZ".equals(mxywlb)) {
        FSSSFJE = MXQSBJ.abs - Fjsf
      }

      //日期、成交金额、佣金、回购收益、初始购回期限、融资利率判断规则
      if ("SFCS".equals(mxywlb) || "SFXZ".equals(mxywlb)) {

        val FLVRDD = csyjlvTableDFBroadCast.value.rdd.filter(row => {
          FZQLB = row.getAs[String]("FZQLB") //证券类别
          FSZSH = row.getAs[String]("FSZSH") //席位地点
          FSTR1 = row.getAs[String]("FSTR1") //席位号
          "ZQZYSFHG".equals(FZQLB) && "S".equals(FSZSH) && FJyxwh.toString.equals(FSTR1)
        })

        var FLV = "0"
        if (FLVRDD.count()==0) {
          var FLV = "0"
        }else{
          FLV = FLVRDD.collect().toBuffer.head.toString()
        }


        FinDate = row.getAs[String]("MXQTRQ") //日期
        Fje = MXQSBJ.abs // 成交金额
        Fyj = Fje.*(Flv).setScale(2, RoundingMode.HALF_UP) // 佣金
        FRZLV = BigDecimal(row.getAs[String]("MXCJJG")) //融资/回购利率
        FHggain = BigDecimal(row.getAs[String]("FHggain")) //回购收益

        FCSGHQX = BigDecimal(row.getAs[String]("FCSGHQXTemp")).setScale(2,RoundingMode.HALF_UP) //初始购回期限

      } else if ("SFDQ".equals(mxywlb) || "SFTG".equals(mxywlb) || "SFJZ".equals(mxywlb)) {

        FinDate = row.getAs[String]("MXCJRQ") //购回日期


        val sjsmxTempArr: Array[Row] = sjsmxTempBrodcast.value.collect()

        for (i <- sjsmxTempArr){

          MXFJSM = i(0).toString
          MXYWLB = i(1).toString
          FJETemp = i(2).toString
          if(FCSHTXH.equals(MXFJSM) && ("SFXZ".equals(MXYWLB) || "SFCS".equals(MXYWLB))){

            Fje = BigDecimal(FJETemp) // 成交/回购金额
            Fyj = BigDecimal(0).setScale(2,RoundingMode.HALF_UP) //拥金
            FHggain = MXQSBJ.abs - Fje //回购收益 是否取绝对值 TODO
            println(Fje)
            val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

            val strYear1=i(3).toString.substring(0,4)
            val strMonth1 = i(3).toString.substring(4,6)
            val strDay1 = i(3).toString.substring(6,8)
            val FJEDate = strYear1 +"-"+ strMonth1 + "-" + strDay1

            val strYear2=FinDate.substring(0,4)
            val strMonth2 =FinDate.substring(4,6)
            val strDay2 = FinDate.substring(6,8)
            val FinDateDate = strYear2 +"-"+ strMonth2 + "-" + strDay2

            val startDate = simpleDateFormat.parse(FJEDate)
            val endDate = simpleDateFormat.parse(FinDateDate)
            FCSGHQX = (endDate.getTime - startDate.getTime) / (24 * 60 * 60 * 1000) //初始购回期限
            //            FCSGHQX.setScale(2,RoundingMode.HALF_UP)
            FRZLV = BigDecimal(0).setScale(2,RoundingMode.HALF_UP) //融资/回购利率
          }
        }


      }

      ShenzhenStockExchangeTriPartyRepoDto(
        FDate,
        FinDate,
        FZqdm,
        FSzsh,
        FJyxwh,
        Fje.toString,
        Fyj.toString(),
        Fjsf.toString(),
        FHggain.toString(),
        FSSSFJE.toString(),
        FZqbz,
        Fjybz,
        FJyFs,
        Fsh,
        Fzzr,
        Fchk,
        FHTXH,
        FSETCODE.toString(),
        FCSGHQX.setScale(2,RoundingMode.HALF_UP).toString(),
        FRZLV.toString(),
        FSJLY,
        FCSHTXH,
        FBS,
        FSL.toString(),
        Fyhs.toString(),
        Fzgf.toString(),
        Fghf.toString(),
        FFxj.toString(),
        FQtf.toString(),
        Fgzlx.toString(),
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
    sjsmxXzxkRDD.toDF().show()
    Util.outputMySql(sjsmxXzxkRDD.toDF(), "mxfdTest_wmz")

    //    return
    //明细-到期续作前期合约了结数据取值规则
    val sjsmxFilterRDD = sjsmxDF.rdd.filter(row => {
      val MXYWLB: String = row.getAs[String]("MXYWLB")
      "SFXZ".equals(MXYWLB)
    })

    println(sjsmxFilterRDD.collect.toBuffer)

    val sjsmxXzljRDD = sjsmxFilterRDD.map(row => {

      FDate = row.getAs[String]("MXCJRQ") //成交日期
      FinDate = row.getAs[String]("MXCJRQ")
      // 初始购回日期
      FJyxwh = row.getAs[String]("MXJYDY") //交易席位号
      FSSSFJE = BigDecimal(row.getAs[String]("MXZJJE")).abs //实收实付金额
      FHTXH= row.getAs[String]("MXYWLSH") //合同序号
      FCSHTXH = row.getAs[String]("MXFJSM") //初始合同序号



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





      //      val sjsmxSFCSRDD = sjsmxDF.rdd.filter(row => {
      //        "SFCS".equals(row.getAs[String]("MXYWLB")) && FCSHTXH.equals(row.getAs[String]("MXFJSM"))
      //      })
      val sjsmxSFCSRDD = sjsmxDFArrBroadCast.value.filter(row =>{
        "SFCS".equals(row.getAs[String]("MXYWLB")) && FCSHTXH.equals(row.getAs[String]("MXFJSM"))
      })


      val SFCSvalue = sjsmxSFCSRDD.map(row => {
        Fje = BigDecimal(row.getAs[String]("FJETemp"))
        FHggain = BigDecimal(row.getAs[String]("FHggain"))
        FRZLV = BigDecimal(row.getAs[String]("FRZLV"))

        Fje + "_" + FHggain + "_" + FRZLV
      })
      // val sjsmxSFCS: Array[String] = SFCSvalue.toString().split("_")
      val sjsmxSFCS: Array[String] = SFCSvalue(0).split("_")

      Fje = BigDecimal(sjsmxSFCS(0)) //成交金额
      FHggain = BigDecimal(sjsmxSFCS(1)) //回购收益
      FRZLV = BigDecimal(sjsmxSFCS(2)) //融资/购回利率

      val FCSGHQX = BigDecimal(row.getAs[String]("FCSGHQXTemp")) //初始购回期限

      var FJyFs: String = new String

      if (BigDecimal(row.getAs[String]("MXQSBJ")) < 0) {
        FJyFs = "RZ" //交易方式
      } else if (BigDecimal(row.getAs[String]("MXQSBJ")) > 0) {
        FJyFs = "CZ" //交易方式
      }

      ShenzhenStockExchangeTriPartyRepoDto(
        FDate,
        FinDate,
        FZqdm,
        FSzsh,
        FJyxwh,
        Fje.toString(),
        Fyj.toString(),
        Fjsf.toString(),
        FHggain.toString(),
        FSSSFJE.toString(),
        FZqbz,
        Fjybz,
        FJyFs,
        Fsh,
        Fzzr,
        Fchk,
        FHTXH,
        FSETCODE.toString(),
        FCSGHQX.toString(),
        FRZLV.toString(),
        FSJLY,
        FCSHTXH,
        FBS,
        FSL.toString(),
        Fyhs.toString(),
        Fzgf.toString(),
        Fghf.toString(),
        FFxj.toString(),
        FQtf.toString(),
        Fgzlx.toString(),
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
    Util.outputMySql(sjsmxXzljRDD.toDF(), "mxdTest_wmz")

    var jgywlb: String = ""


    //结果-非到期续作前期合约了结数据取值规则
    val sjsjgXzxkRDD = sjsjgDF.rdd.map(row => {

      jgywlb = row.getAs[String]("JGYWLB") // 明细业务类别
      FDate = row.getAs[String]("JGCJRQ") //日期
      FJyxwh = row.getAs[String]("JGJYDY") //交易席位号
      FHTXH = row.getAs[String]("JGYWLSH") //合同序号
      FCSHTXH = row.getAs[String]("JGFJSM") //初始合同序号

      val Fjsf: BigDecimal = BigDecimal(row.getAs[String]("JGJYJSF")).abs //经手费

      val FZqdm: String = " " //证券代码
      val FSzsh: String = "S" //交易市场
      val FZqbz: String = "ZQ" //证券表示
      val Fsh: Int = 1 //审核
      val Fzzr: String = "admin" //制作人:当前用户
      val Fchk: String = "admin" //审核人:当前用户
      val FSJLY: String = "ZD" //数据来源


      var Fjybz: String = "" //交易标志
      var FJyFs: String = "" //交易方式
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
      val JGQSBJ: BigDecimal = BigDecimal(row.getAs[String]("JGQSBJ"))
      if ("SFCS".equals(jgywlb)) {
        if (JGQSBJ > 0) {
          FJyFs = "RZ"
        } else if (JGQSBJ < 0) {
          FJyFs = "CZ"
        }
      } else if ("SFDQ SFTG SFJZ".contains(jgywlb)) {
        if (JGQSBJ < 0) {
          FJyFs = "RZ"
        } else if (JGQSBJ > 0) {
          FJyFs = "CZ"
        }
      } else if ("SFXZ".equals(jgywlb)) {
        if (JGQSBJ > 0) {
          FJyFs = "RZ"
        } else if (JGQSBJ < 0) {
          FJyFs = "CZ"
        }
      }

      // 实收实付金额判断规则
      if ("SFCS".equals(jgywlb) || "SDFQ".equals(jgywlb) || "SFTG".equals(jgywlb) || "SFJZ".equals(jgywlb)) {
        FSSSFJE = BigDecimal(row.getAs[String]("JGSFJE")).abs
      } else if ("SFXZ".equals(jgywlb)) {
        FSSSFJE = JGQSBJ.abs - Fjsf
      }

      //日期、成交金额、佣金、回购收益、初始购回期限、融资利率判断规则
      if ("SFCS".equals(jgywlb) || "SFXZ".equals(jgywlb)) {

        val FLVRDD = csyjlvTableDFBroadCast.value.rdd.filter(row => {
          FZQLB = row.getAs[String]("FZQLB") //证券类别
          FSZSH = row.getAs[String]("FSZSH") //席位地点
          FSTR1 = row.getAs[String]("FSTR1") //席位号
          "ZQZYSFHG".equals(FZQLB) && "S".equals(FSZSH) && FJyxwh.equals(FSTR1)
        })

        var FLV = "0"
        if (FLVRDD.count()==0) {
          var FLV = "0"
        }else{
          FLV = FLVRDD.collect().toBuffer.head.toString()
        } // 佣金费率

        FinDate = row.getAs[String]("JGQTRQ") //日期
        Fje = JGQSBJ.abs // 成交金额
        Fyj = Fje.*(Flv).setScale(2, RoundingMode.HALF_UP) // 佣金
        FRZLV = BigDecimal(row.getAs[String]("JGCJJG")) //融资/回购利率
        FHggain = BigDecimal(row.getAs[String]("FHggain")) //回购收益

        //        val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        //        val endDate = simpleDateFormat.parse(FinDate)
        //        val startDate = simpleDateFormat.parse(row.getAs[String]("MXCJRQ"))
        //
        //        FCSGHQX = (endDate - startDate)/(24*60*60*1000) //初试购回期限

        FCSGHQX = BigDecimal(row.getAs[String]("FCSGHQXTemp")) //初始购回期限

      } else if ("SFDQ".equals(jgywlb) || "SFTG".equals(jgywlb) || "SFJZ".equals(jgywlb)) {
        FinDate = row.getAs[String]("JGCJRQ") //购回日期

        //获取对应初试合同序号的业务类别为SFXZ的回购业务
        val FjeArr = sjsjgTempBroadcast.value.filter(row => {
          JGFJSM = row.getAs[String]("JGFJSM")
          JGYWLB = row.getAs[String]("JGYWLB")
          FJETemp = row.getAs[String]("FJETemp")

          FCSHTXH.equals(JGFJSM) && "SFXZ".equals(JGYWLB) || "SFCS".equals(JGYWLB)
        })


        //获取广播变量
        val sjsjgTempArr: Array[Row] = sjsjgTempBroadcast.value.collect()

        for (i <- sjsjgTempArr){

          JGFJSM = i(0).toString
          JGYWLB = i(1).toString
          FJETemp = i(2).toString
          if(FCSHTXH.equals(JGFJSM) && ("SFXZ".equals(JGYWLB) || "SFCS".equals(JGYWLB))){

            Fje = BigDecimal(FJETemp) // 成交/回购金额
            Fyj = BigDecimal(0).setScale(2,RoundingMode.HALF_UP) //拥金
            FHggain = JGQSBJ.abs - Fje //回购收益 是否取绝对值 TODO
            println(Fje)
            val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

            val strYear1=i(3).toString.substring(0,4)
            val strMonth1 = i(3).toString.substring(4,6)
            val strDay1 = i(3).toString.substring(6,8)
            val FJEDate = strYear1 +"-"+ strMonth1 + "-" + strDay1

            val strYear2=FinDate.substring(0,4)
            val strMonth2 =FinDate.substring(4,6)
            val strDay2 = FinDate.substring(6,8)
            val FinDateDate = strYear2 +"-"+ strMonth2 + "-" + strDay2

            val startDate = simpleDateFormat.parse(FJEDate)
            val endDate = simpleDateFormat.parse(FinDateDate)
            FCSGHQX = (endDate.getTime - startDate.getTime) / (24 * 60 * 60 * 1000) //初始购回期限
            FRZLV = BigDecimal(0).setScale(2,RoundingMode.HALF_UP) //融资/回购利率
          }
        }

        FRZLV = BigDecimal(0) //融资/回购利率
      }

      ShenzhenStockExchangeTriPartyRepoDto(
        FDate,
        FinDate,
        FZqdm,
        FSzsh,
        FJyxwh,
        Fje.toString(),
        Fyj.toString(),
        Fjsf.toString(),
        FHggain.toString(),
        FSSSFJE.setScale(2,RoundingMode.HALF_UP).toString(),
        FZqbz,
        Fjybz,
        FJyFs,
        Fsh,
        Fzzr,
        Fchk,
        FHTXH,
        FSETCODE.toString(),
        FCSGHQX.setScale(2,RoundingMode.HALF_UP).toString(),
        FRZLV.setScale(2,RoundingMode.HALF_UP).toString(),
        FSJLY,
        FCSHTXH,
        FBS,
        FSL.toString(),
        Fyhs.toString(),
        Fzgf.toString(),
        Fghf.toString(),
        FFxj.toString(),
        FQtf.toString(),
        Fgzlx.toString(),
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
    Util.outputMySql(sjsjgXzxkRDD.toDF(), "jgfdTest_wmz")

    //结果-到期续作前期合约了结数据取值规则
    val sjsjgFilterRDD = sjsjgDF.rdd.filter(row => {
      val JGYWLB: String = row.getAs[String]("JGYWLB")
      "SFXZ".equals(JGYWLB)
    })

    val sjsjgXzljRDD = sjsjgFilterRDD.map(row => {

      FDate = row.getAs[String]("JGCJRQ") //成交日期
      FinDate = row.getAs[String]("JGCJRQ")
      // 初始购回日期
      FJyxwh = row.getAs[String]("JGJYDY") //交易席位号
      FSSSFJE = BigDecimal(row.getAs[String]("JGZJJE")).abs //实收实付金额
      FHTXH = row.getAs[String]("JGYWLSH") //合同序号
      FCSHTXH = row.getAs[String]("JGFJSM") //初始合同序号


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


      val sjsjgSFCSArr = sjsjgDFArrBroadCast.value.filter(row => {
        "SFCS".equals(row.getAs[String]("JGYWLB")) && FCSHTXH.equals(row.getAs[String]("JGFJSM"))
      })


      val SFCSvalue = sjsjgSFCSArr.map(row => {
        Fje = BigDecimal(row.getAs[String]("FJETemp"))
        FHggain = BigDecimal(row.getAs[String]("FHggain"))
        FRZLV = BigDecimal(row.getAs[String]("FRZLV"))

        Fje + "_" + FHggain + "_" + FRZLV
      })
      val sjsjgSFCS: Array[String] = SFCSvalue(0).split("_")

      Fje = BigDecimal(sjsjgSFCS(0)) //成交金额
      FHggain = BigDecimal(sjsjgSFCS(1)) //回购收益
      FRZLV = BigDecimal(sjsjgSFCS(2)) //融资/购回利率

      FCSGHQX = BigDecimal(row.getAs[String]("FCSGHQXTemp")) //初始购回期限

      var FJyFs: String = ""

      if (BigDecimal(row.getAs[String]("JGQSBJ")) < 0) {
        FJyFs = "RZ" //交易方式
      } else if (BigDecimal(row.getAs[String]("JGQSBJ")) > 0) {
        FJyFs = "CZ" //交易方式
      }

      ShenzhenStockExchangeTriPartyRepoDto(
        FDate,
        FinDate,
        FZqdm,
        FSzsh,
        FJyxwh,
        Fje.toString(),
        Fyj.toString(),
        Fjsf.toString(),
        FHggain.toString(),
        FSSSFJE.setScale(2,RoundingMode.HALF_UP).toString(),
        FZqbz,
        Fjybz,
        FJyFs,
        Fsh,
        Fzzr,
        Fchk,
        FHTXH,
        FSETCODE.toString(),
        FCSGHQX.setScale(2,RoundingMode.HALF_UP).toString(),
        FRZLV.setScale(2,RoundingMode.HALF_UP).toString(),
        FSJLY,
        FCSHTXH,
        FBS,
        FSL.toString(),
        Fyhs.toString(),
        Fzgf.toString(),
        Fghf.toString(),
        FFxj.toString(),
        FQtf.toString(),
        Fgzlx.toString(),
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
    Util.outputMySql(sjsjgXzljRDD.toDF(), "jgdTest_wmz")

  }
}
