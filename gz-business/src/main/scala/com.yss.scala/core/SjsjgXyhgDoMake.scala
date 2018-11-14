package com.yss.scala.core

import java.text.SimpleDateFormat

import com.yss.scala.core.SjsjgContants.DEFAULT_VALUE_SPACE
import com.yss.scala.dbf.dbf._
import com.yss.scala.dto.{Hzjkqs, SJSJGETL}
import com.yss.scala.util.{DateUtils, BasicUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.math.BigDecimal.RoundingMode
/**
  * @author erwa he
  * @version 2018-11-06
  *          describe: 深圳债券质押式协议回购交易
  *          目标文件：sjsjg文件
  *          目标表：
  */

object SjsjgXyhgDoMake {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SjsjgXyhgDoMake")
      .master("local[*]")
      .getOrCreate()


    //原始数据
    val sjsjg: DataFrame = spark.sqlContext.dbfFile("hdfs://192.168.102.120:8020/tmp/wmz/SJSJG")

    //处理符合条件的数据
    val sjsjgFilterRDD = sjsjg.rdd.filter(row => {
      "XYCS XYDQ".contains(row.getAs[String]("JGYWLB")) &&
        "02".equals(row.getAs[String]("JGJYFS")) &&
        "01".equals(row.getAs[String]("JGSJLX")) &&
        "Y".equals(row.getAs[String]("JGJSBZ"))
    })

    val JGvalueRDD = sjsjgFilterRDD.map(lines => {
      // 原数据
      val JGJSZH: String = lines.getAs[String]("JGJSZH")
      val JGBFZH: String = lines.getAs[String]("JGBFZH")
      val JGSJLX: String = lines.getAs[String]("JGSJLX")
      val JGYWLB: String = lines.getAs[String]("JGYWLB")
      val JGZQDM: String = lines.getAs[String]("JGZQDM")
      val JGJYDY: String = lines.getAs[String]("JGJYDY")
      val JGTGDY: String = lines.getAs[String]("JGTGDY")
      val JGZQZH: String = lines.getAs[String]("JGZQZH")
      val JGDDBH: String = lines.getAs[String]("JGDDBH")
      val JGYYB: String = lines.getAs[String]("JGYYB")
      val JGZXBH: String = lines.getAs[String]("JGZXBH")
      val JGYWLSH: String = lines.getAs[String]("JGYWLSH")
      val JGCJSL: String = lines.getAs[String]("JGCJSL")
      val JGQSSL: String = lines.getAs[String]("JGQSSL")
      val JGJSSL: String = lines.getAs[String]("JGJSSL")
      val JGCJJG: String = lines.getAs[String]("JGCJJG")
      val JGQSJG: String = lines.getAs[String]("JGQSJG")
      val JGXYJY: String = lines.getAs[String]("JGXYJY")
      val JGPCBS: String = lines.getAs[String]("JGPCBS")
      val JGZQLB: String = lines.getAs[String]("JGZQLB")
      val JGZQZL: String = lines.getAs[String]("JGZQZL")
      val JGGFXZ: String = lines.getAs[String]("JGGFXZ")
      val JGLTLX: String = lines.getAs[String]("JGLTLX")
      val JGJSFS: String = lines.getAs[String]("JGJSFS")
      val JGHBDH: String = lines.getAs[String]("JGHBDH")
      val JGQSBJ: String = lines.getAs[String]("JGQSBJ")
      val JGYHS: String = lines.getAs[String]("JGYHS")
      val JGJYJSF: String = lines.getAs[String]("JGJYJSF")
      val JGJGGF: String = lines.getAs[String]("JGJGGF")
      val JGGHF: String = lines.getAs[String]("JGGHF")
      val JGJSF: String = lines.getAs[String]("JGJSF")
      val JGSXF: String = lines.getAs[String]("JGSXF")
      val JGQSYJ: String = lines.getAs[String]("JGQSYJ")
      val JGQTFY: String = lines.getAs[String]("JGQTFY")
      val JGZJJE: String = lines.getAs[String]("JGZJJE")
      val JGSFJE: String = lines.getAs[String]("JGSFJE")
      val JGJSBZ: String = lines.getAs[String]("JGJSBZ")
      val JGZYDH: String = lines.getAs[String]("JGZYDH")
      val JGCJRQ: String = lines.getAs[String]("JGCJRQ")
      val JGQSRQ: String = lines.getAs[String]("JGQSRQ")
      val JGJSRQ: String = lines.getAs[String]("JGJSRQ")
      val JGFSRQ: String = lines.getAs[String]("JGFSRQ")
      val JGQTRQ: String = lines.getAs[String]("JGQTRQ")
      val JGSCDM: String = lines.getAs[String]("JGSCDM")
      val JGJYFS: String = lines.getAs[String]("JGJYFS")
      val JGZQDM2: String = lines.getAs[String]("JGZQDM2")
      val JGTGDY2: String = lines.getAs[String]("JGTGDY2")
      val JGDDBH2: String = lines.getAs[String]("JGDDBH2")
      val JGFJSM: String = lines.getAs[String]("JGFJSM")
      val JGBYBZ: String = lines.getAs[String]("JGBYBZ")

      SJSJGETL(JGJSZH,
        JGBFZH,
        JGSJLX,
        JGYWLB,
        JGZQDM,
        JGJYDY,
        JGTGDY,
        JGZQZH,
        JGDDBH,
        JGYYB,
        JGZXBH,
        JGYWLSH,
        JGCJSL,
        JGQSSL,
        JGJSSL,
        JGCJJG,
        JGQSJG,
        JGXYJY,
        JGPCBS,
        JGZQLB,
        JGZQZL,
        JGGFXZ,
        JGLTLX,
        JGJSFS,
        JGHBDH,
        JGQSBJ,
        JGYHS,
        JGJYJSF,
        JGJGGF,
        JGGHF,
        JGJSF,
        JGSXF,
        JGQSYJ,
        JGQTFY,
        JGZJJE,
        JGSFJE,
        JGJSBZ,
        JGZYDH,
        JGCJRQ,
        JGQSRQ,
        JGJSRQ,
        JGFSRQ,
        JGQTRQ,
        JGSCDM,
        JGJYFS,
        JGZQDM2,
        JGTGDY2,
        JGDDBH2,
        JGFJSM,
        JGBYBZ
      )
    })
    import spark.implicits._
    //初始交易、到期交易数据写入mysql数据库，方便校验
    BasicUtils.outputMySql(JGvalueRDD.toDF(), "SJSJGXYCSDQAllData")

    //计算规则方法
    calculator(JGvalueRDD.toDF(),spark )

    spark.stop()

  }

  /**
    * 计算规则方法
    */

  private def calculator(sjsjgDF:DataFrame,spark: SparkSession): Unit = {

    val sc = spark.sparkContext

    sjsjgDF.createOrReplaceTempView("sjsjgAllFlag")
    val sjsjgTempDF: DataFrame = spark.sqlContext.sql("select JGFJSM,JGYWLB,JGCJRQ from sjsjgAllFlag")
    val sjsjgTempArrBroadcast = sc.broadcast(sjsjgTempDF.collect())

    val day = DateUtils.formatDate(System.currentTimeMillis())
    // 读取费率表
    val csyjlv: RDD[String] = sc.textFile("hdfs://192.168.102.120:8020/yss/guzhi/basic_list/" + day + "/CSYJLV/part-m-00000")
    // 将费率表转换成DateFrame
    val schemaString: String = "FID FZQLB FSZSH FLV FLVMIN FTJ1 FSTR1 FTJ2 FTJ2FROM FTJ2TO FLVZK FSH FZZR FCHK FSTARTDATE FJJDM FGDJE"
    // nullable = true 指可以为空
    val fields: Array[StructField] = schemaString.split(" ").map(fieldname => StructField(fieldname, StringType, nullable = true))
    //  设置文件头信息
    val schema = StructType(fields)
    val rowRDD: RDD[Row] = csyjlv.map(_.split(",")).map(attributes => Row(attributes(0).trim, attributes(1).trim, attributes(2).trim, attributes(3).trim, attributes(4).trim, attributes(5).trim, attributes(6).trim, attributes(7).trim, attributes(8).trim, attributes(9).trim, attributes(10).trim, attributes(11).trim, attributes(12).trim, attributes(13).trim, attributes(14).trim, attributes(15).trim, attributes(16).trim))
    val csyjlvDF: DataFrame = spark.createDataFrame(rowRDD, schema)

    //生成临时表并筛选数据
    csyjlvDF.createOrReplaceTempView("csyjlvflag")
    val csyjlvTableDF: DataFrame = spark.sqlContext.sql("select FZQLB,FSZSH,FSTR1,FLV from csyjlvflag")
    // 广播出去
    val csyjlvTableArr: Array[Row] = csyjlvTableDF.rdd.collect()

    val csyjlvTableArrBroadCast = sc.broadcast(csyjlvTableArr)

    var FDate: String = ""
    var FJyxwh: String = ""
    var FHTXH: String = ""
    var FCSHTXH: String = ""
    var FLV: BigDecimal = BigDecimal(0)
    var FZQLB: String = ""
    var FSZSH: String = ""
    var FSTR1: String = ""
    var JGFJSM: String = ""
    var JGYWLB: String = ""
    var FHggain: BigDecimal = BigDecimal(0)
    var jgywlb: String = ""


    val sjsjgXzxkRDD = sjsjgDF.rdd.map(row => {

      jgywlb = row.getAs[String]("JGYWLB") // 明细业务类别
      FDate = row.getAs[String]("JGCJRQ") //日期
      FJyxwh = row.getAs[String]("JGJYDY") //交易席位号
      FHTXH = row.getAs[String]("JGYWLSH") //合同序号
      FCSHTXH = row.getAs[String]("JGFJSM") //初始合同序号
      val Fjsf: BigDecimal = BigDecimal(row.getAs[String]("JGJYJSF")).abs //经手费
      val FZqdm: String = row.getAs[String]("JGZQDM") //证券代码
      val FSzsh: String = "S" //交易市场
      val FZqbz: String = " " //证券表示
      val Fsh: String = "1" //审核
      val Fzzr: String = " " //制作人:当前用户
      val Fchk: String = " " //审核人:当前用户
      val FSJLY: String = "ZD" //数据来源
      var Fjybz: String = "" //交易标志
      var FJyFs: String = "" //交易方式
      var FinDate: String = new String //日期
      var Fje: BigDecimal = BigDecimal(0) //成交金额
      var Fyj: BigDecimal = BigDecimal(0) //佣金
      var FSSSFJE: BigDecimal = BigDecimal(0) //实收实付金额
      var FRZLV: BigDecimal = BigDecimal(0) //融资/回购利率
      var FCSGHQX: BigDecimal = BigDecimal(0) //初始购回期限
      val FSETID = BigDecimal(0) // 资产代码
      var FSL = BigDecimal(0) // 成交数量
      val Fyhs = BigDecimal(0) // 印花税
      val Fzgf = BigDecimal(0) //征管费
      val Fghf = BigDecimal(0) //过户费
      val FFxj = BigDecimal(0) // 风险金
      val FQtf = BigDecimal(0) // 其他费用
      val Fgzlx = BigDecimal(0) //国债利息
      val FQsbz = "" //清算标志
      val Ftzbz = " " //投资标志
      val FQsghf = "0" //券商过户费
      var FGddm = "" //股东代码
      val fzlh = " " //指令号
      val ISRTGS = "" //结算方式
      val FPARTID = "" // 结算会员
      val FYwbz = " " // 业务标志
      val Fbz = "" // 币种
      var ZqDm = "" //证券代码
      val FBS = "" // 买卖方向

      //交易标志判断规则
      jgywlb match {
        case "XYCS" => Fjybz = "XYCS"
        case "XYDQ" => Fjybz = "XYDQ"
      }

      //交易方式判断规则
      val JGQSBJ: BigDecimal = BigDecimal(row.getAs[String]("JGQSBJ"))
      val JGCJSL: BigDecimal = BigDecimal(row.getAs[String]("JGCJSL"))
      val JGQSSL: BigDecimal = BigDecimal(row.getAs[String]("JGQSSL"))
      val JGJSSL: BigDecimal = BigDecimal(row.getAs[String]("JGJSSL"))
      if ("XYCS".equals(jgywlb)) {
        if ((JGQSBJ > 0 || JGQSBJ==0) && JGCJSL < 0 && JGQSSL < 0 && JGJSSL < 0) {
          FJyFs = "RZ"
        } else if ((JGQSBJ < 0 || JGQSBJ==0) && JGCJSL > 0 && JGQSSL == 0 && JGJSSL == 0) {
          FJyFs = "CZ"
        }
      } else if ("XYDQ".contains(jgywlb)) {
        if ((JGQSBJ < 0 || JGQSBJ==0) && JGCJSL == 0 && JGQSSL > 0 && JGJSSL > 0) {
          FJyFs = "RZ"
        } else if ((JGQSBJ > 0 || JGQSBJ==0) && JGCJSL == 0 && JGQSSL == 0 && JGJSSL == 0) {
          FJyFs = "CZ"
        }
      }

      // 实收实付金额判断规则
      if ("XYCS".equals(jgywlb) && "RZ".equals(FJyFs)) {
        FSSSFJE = JGQSBJ.abs - Fjsf
      } else if ("XYCS".equals(jgywlb) && "CZ".equals(FJyFs)) {
        FSSSFJE = JGQSBJ.abs + Fjsf
      } else if ("XYDQ".equals(jgywlb) ) {
        FSSSFJE = JGQSBJ.abs
      }

      FHggain = BigDecimal(row.getAs[String]("FHggain")) //回购收益
      Fje = JGQSBJ.abs // 成交金额

      FSL = BigDecimal(row.getAs[String]("JGJSSL"))
      FGddm = row.getAs[String]("JGZQZH")
      FRZLV = BigDecimal(row.getAs[String]("JGCJJG"))/100  //融资/回购利率
      //日期、初始购回期限的判断规则
      if ("XYCS".equals(jgywlb) ) {

        val FLVRDD = csyjlvTableArrBroadCast.value.filter(row => {
          FZQLB = row.getAs[String]("FZQLB") //证券类别
          FSZSH = row.getAs[String]("FSZSH") //席位地点
          FSTR1 = row.getAs[String]("FSTR1") //席位号
          "ZQZYHG".equals(FZQLB) && "S".equals(FSZSH) && FJyxwh.equals(FSTR1)
        })

        // 佣金费率
        if (FLVRDD.length == 0) {
          FLV = 0
        } else {
          FLV = FLVRDD.head.getAs("FLV")
        }

        FinDate = row.getAs[String]("JGQTRQ") //日期
        Fyj = Fje.*(FLV).setScale(2, RoundingMode.HALF_UP) // 佣金

        val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val strYear1 = FDate.toString.substring(0, 4)
        val strMonth1 = FDate.toString.substring(4, 6)
        val strDay1 = FDate.toString.substring(6, 8)
        val FJEDate = strYear1 + "-" + strMonth1 + "-" + strDay1

        val strYear2 = FinDate.substring(0, 4)
        val strMonth2 = FinDate.substring(4, 6)
        val strDay2 = FinDate.substring(6, 8)
        val FinDateDate = strYear2 + "-" + strMonth2 + "-" + strDay2

        val startDate = simpleDateFormat.parse(FJEDate)
        val endDate = simpleDateFormat.parse(FinDateDate)
        FCSGHQX = (endDate.getTime - startDate.getTime) / (24 * 60 * 60 * 1000) //初始购回期限
        ZqDm = FCSGHQX.toString()

      } else if ("XYDQ".equals(jgywlb)) {
        FinDate = row.getAs[String]("JGCJRQ") //购回日期

        //获取广播变量
        val sjsjgTempArr: Array[Row] = sjsjgTempArrBroadcast.value

        for (i <- sjsjgTempArr) {

          JGFJSM = i(0).toString
          JGYWLB = i(1).toString
          if (FCSHTXH.equals(JGFJSM) && "XYCS".equals(JGYWLB)) {


            val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

            val strYear1 = i(2).toString.substring(0, 4)
            val strMonth1 = i(2).toString.substring(4, 6)
            val strDay1 = i(2).toString.substring(6, 8)
            val FJEDate = strYear1 + "-" + strMonth1 + "-" + strDay1

            val strYear2 = FinDate.substring(0, 4)
            val strMonth2 = FinDate.substring(4, 6)
            val strDay2 = FinDate.substring(6, 8)
            val FinDateDate = strYear2 + "-" + strMonth2 + "-" + strDay2

            val startDate = simpleDateFormat.parse(FJEDate)
            val endDate = simpleDateFormat.parse(FinDateDate)
            FCSGHQX = (endDate.getTime - startDate.getTime) / (24 * 60 * 60 * 1000) //初始购回期限
            ZqDm = FCSGHQX.toString()

          }
        }
      }

      Hzjkqs(
        FSETID.toString(),
        FDate,
        FinDate,
        FZqdm,
        FSzsh,
        FJyxwh,
        FBS,
        Fje.toString(),
        FSL.toString(),
        Fyj.toString(),
        Fjsf.toString(),
        Fyhs.toString(),
        Fzgf.toString(),
        Fghf.toString(),
        FFxj.toString(),
        FQtf.toString(),
        Fgzlx.toString(),
        FHggain.toString(),
        FSSSFJE.setScale(2, RoundingMode.HALF_UP).toString(),
        FZqbz,
        FYwbz,
        Fjybz,
        FQsbz,
        ZqDm,
        FJyFs,
        Fsh,
        Fzzr,
        Fchk,
        fzlh,
        Ftzbz,
        FQsghf,
        FGddm,
        ISRTGS,
        FPARTID,
        FHTXH,
        FCSHTXH,
        FRZLV.setScale(4, RoundingMode.HALF_UP).toString(),
        FCSGHQX.setScale(2, RoundingMode.HALF_UP).toString(),
        FSJLY,
        Fbz,
        DEFAULT_VALUE_SPACE,
        DEFAULT_VALUE_SPACE,
        DEFAULT_VALUE_SPACE,
        DEFAULT_VALUE_SPACE,
        DEFAULT_VALUE_SPACE
      )
    })

    import spark.implicits._
    BasicUtils.outputMySql(sjsjgXzxkRDD.toDF(), "SJSJGXYCSDQ")
  }
}