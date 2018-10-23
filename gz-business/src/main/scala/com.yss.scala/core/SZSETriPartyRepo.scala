package com.yss.scala.core

import java.text.SimpleDateFormat
import java.util.Properties

import com.yss.scala.dbf.dbf._
import com.yss.scala.dto.{JGETL, MXETL, SZSETriPartyRepoDto}
import com.yss.scala.util.{DateUtils, Util}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import sun.management.snmp.AdaptorBootstrap.DefaultValues

import scala.math.BigDecimal.RoundingMode

/**
  * @author MingZhang Wang
  * @version 2018-09-17 16:37
  *          describe: 三方回购业务(深交所) ETL 加入新字段, 并计算需要提前计算的字段, 如:"SFCS"时的成交金额, 购回期限, 回购收益
  *          目标文件：sjsjg,sjsmx下所有文件
  *          目标表：
  *
  *
  */
object SZSETriPartyRepo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SZSETriPartyRepo")
//      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    //原始数据
    val sjsjg: DataFrame = spark.sqlContext.dbfFile("hdfs://192.168.102.120:8020/tmp/wmz/SFHG-JG")
    val sjsmx: DataFrame = spark.sqlContext.dbfFile("hdfs://192.168.102.120:8020/tmp/wmz/SFHG-MX")

//    sjsjg.show()

    val sjsmxFilterRDD = sjsmx.rdd.filter(row => {
//      "20180723".equals(row.getAs[String]("MXCJRQ")) &&
        "SFCS SFDQ SFXZ SFTG SFJZ".contains(row.getAs[String]("MXYWLB")) &&
        "02".equals(row.getAs[String]("MXJYFS")) &&
        "01".equals(row.getAs[String]("MXSJLX"))
    })

    val sjsjgFilterRDD = sjsjg.rdd.filter(row => {
//      "20180723".equals(row.getAs[String]("JGCJRQ")) &&
        "SFCS SFDQ SFXZ SFTG SFJZ".contains(row.getAs[String]("JGYWLB")) &&
        "02".equals(row.getAs[String]("JGJYFS")) &&
        "01".equals(row.getAs[String]("JGSJLX"))
    })

    val MXvalueRDD = sjsmxFilterRDD.map(lines => {

      // 原数据
      val MXJSZH: String = lines.getAs[String]("MXJSZH")
      val MXBFZH: String = lines.getAs[String]("MXBFZH")
      val MXSJLX: String = lines.getAs[String]("MXSJLX")
      val MXYWLB: String = lines.getAs[String]("MXYWLB")
      val MXZQDM: String = lines.getAs[String]("MXZQDM")
      val MXJYDY: String = lines.getAs[String]("MXJYDY")
      val MXTGDY: String = lines.getAs[String]("MXTGDY")
      val MXZQZH: String = lines.getAs[String]("MXZQZH")
      val MXDDBH: String = lines.getAs[String]("MXDDBH")
      val MXYYB: String = lines.getAs[String]("MXYYB")
      val MXZXBH: String = lines.getAs[String]("MXZXBH")
      val MXYWLSH: String = lines.getAs[String]("MXYWLSH")
      val MXCJSL: String = lines.getAs[String]("MXCJSL")
      val MXQSSL: String = lines.getAs[String]("MXQSSL")
      val MXCJJG: String = lines.getAs[String]("MXCJJG")
      val MXQSJG: String = lines.getAs[String]("MXQSJG")
      val MXXYJY: String = lines.getAs[String]("MXXYJY")
      val MXPCBS: String = lines.getAs[String]("MXPCBS")
      val MXZQLB: String = lines.getAs[String]("MXZQLB")
      val MXZQZL: String = lines.getAs[String]("MXZQZL")
      val MXGFXZ: String = lines.getAs[String]("MXGFXZ")
      val MXJSFS: String = lines.getAs[String]("MXJSFS")
      val MXHBDH: String = lines.getAs[String]("MXHBDH")
      val MXQSBJ: String = lines.getAs[String]("MXQSBJ")
      var MXYHS: String = getRowFieldAsString(lines,"MXYHS","0.00")
      var MXJYJSF: String = getRowFieldAsString(lines, "MXJYJSF", "0.00")
      val MXJGGF: String = lines.getAs[String]("MXJGGF")
      val MXGHF: String = lines.getAs[String]("MXGHF")
      val MXJSF: String = lines.getAs[String]("MXJSF")
      val MXSXF: String = lines.getAs[String]("MXSXF")
      val MXQSYJ: String = lines.getAs[String]("MXQSYJ")
      val MXQTFY: String = lines.getAs[String]("MXQTFY")
      val MXZJJE: String = lines.getAs[String]("MXZJJE")
      val MXSFJE: String = lines.getAs[String]("MXSFJE")
      val MXCJRQ: String = lines.getAs[String]("MXCJRQ")
      val MXQSRQ: String = lines.getAs[String]("MXQSRQ")
      val MXJSRQ: String = lines.getAs[String]("MXJSRQ")
      val MXFSRQ: String = lines.getAs[String]("MXFSRQ")
      val MXQTRQ: String = lines.getAs[String]("MXQTRQ")
      val MXSCDM: String = lines.getAs[String]("MXSCDM")
      val MXJYFS: String = lines.getAs[String]("MXJYFS")
      val MXZQDM2: String = lines.getAs[String]("MXZQDM")
      val MXTGDY2: String = lines.getAs[String]("MXTGDY")
      val MXDDBH2: String = lines.getAs[String]("MXDDBH")
      val MXCWDH: String = lines.getAs[String]("MXCWDH")
      val MXPPHM: String = lines.getAs[String]("MXPPHM")
      val MXFJSM: String = lines.getAs[String]("MXFJSM")
      val MXBYBZ: String = lines.getAs[String]("MXBYBZ")

      // 新加字段
      val FYWBZ: String = new String // 业务标志
      val FJYBZ: String = new String // 交易标志
      val FJETemp: BigDecimal = BigDecimal(MXQSBJ).abs // "SFCS"时的成交金额

      val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

      val strYear1 = MXQTRQ.substring(0, 4)
      val strMonth1 = MXQTRQ.substring(4, 6)
      val strDay1 = MXQTRQ.substring(6, 8)
      val MXQTRQDate = strYear1 + "-" + strMonth1 + "-" + strDay1

      val strYear2 = MXCJRQ.substring(0, 4)
      val strMonth2 = MXCJRQ.substring(4, 6)
      val strDay2 = MXCJRQ.substring(6, 8)
      val MXCJRQDate = strYear2 + "-" + strMonth2 + "-" + strDay2

      val endDate = simpleDateFormat.parse(MXQTRQDate)
      val startDate = simpleDateFormat.parse(MXCJRQDate)

      val FCSGHQXTemp: BigDecimal = BigDecimal((endDate.getTime - startDate.getTime)./(24 * 60 * 60 * 1000)) //"SFCS"时的购回期限 = 购回日期 - 首期日期

      val FRZLV: BigDecimal = BigDecimal(MXCJJG) // 回购利率
      val FHggain: BigDecimal = FJETemp.*(FRZLV).*(FCSGHQXTemp)./(365).setScale(2, RoundingMode.HALF_UP) // 回购收益 = 回购金额×回购利率*购回期限/365；

      MXETL(MXJSZH, MXBFZH, MXSJLX, MXYWLB, MXZQDM, MXJYDY, MXTGDY, MXZQZH, MXDDBH, MXYYB, MXZXBH, MXYWLSH, MXCJSL, MXQSSL, MXCJJG, MXQSJG, MXXYJY, MXPCBS, MXZQLB, MXZQZL, MXGFXZ, MXJSFS, MXHBDH, MXQSBJ, MXYHS, MXJYJSF, MXJGGF, MXGHF, MXJSF, MXSXF, MXQSYJ, MXQTFY, MXZJJE, MXSFJE, MXCJRQ, MXQSRQ, MXJSRQ, MXFSRQ, MXQTRQ, MXSCDM, MXJYFS, MXZQDM2, MXTGDY2, MXDDBH2, MXCWDH, MXPPHM, MXFJSM, MXBYBZ, FYWBZ, FJYBZ, FJETemp.setScale(2, RoundingMode.HALF_UP).toString(), FCSGHQXTemp.setScale(2, RoundingMode.HALF_UP).toString(), FHggain.setScale(2, RoundingMode.HALF_UP).toString(), FRZLV.setScale(4, RoundingMode.HALF_UP).toString())

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

      //新加字段
      val FYWBZ: String = new String // 业务标志
      val FJYBZ: String = new String // 交易标志
      val FJETemp: BigDecimal = BigDecimal(JGQSBJ).abs // "SFCS"时的成交金额

      val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

      val strYear1 = JGQTRQ.substring(0, 4)
      val strMonth1 = JGQTRQ.substring(4, 6)
      val strDay1 = JGQTRQ.substring(6, 8)
      val JGQTRQDate = strYear1 + "-" + strMonth1 + "-" + strDay1


      val strYear2 = JGCJRQ.substring(0, 4)
      val strMonth2 = JGCJRQ.substring(4, 6)
      val strDay2 = JGCJRQ.substring(6, 8)
      val JGCJRQDate = strYear2 + "-" + strMonth2 + "-" + strDay2

      val endDate = simpleDateFormat.parse(JGQTRQDate)
      val startDate = simpleDateFormat.parse(JGCJRQDate)

      val FCSGHQXTemp: BigDecimal = (endDate.getTime - startDate.getTime) / (24 * 60 * 60 * 1000) //"SFCS"时的购回期限 = 购回日期 - 首期日期
      val FRZLV: BigDecimal = BigDecimal(JGCJJG)// 回购利率
      val FHggain: BigDecimal = FJETemp.*(FRZLV).*(FCSGHQXTemp)./(365).setScale(2, RoundingMode.HALF_UP) // 回购收益 = 回购金额×回购利率*购回期限/365；

      JGETL(JGJSZH,
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
        JGBYBZ,
        FYWBZ,
        FJYBZ,
        FJETemp.setScale(2,RoundingMode.HALF_UP).toString(),
        FCSGHQXTemp.setScale(2,RoundingMode.HALF_UP).toString(),
        FHggain.setScale(2,RoundingMode.HALF_UP).toString(),
        FRZLV.setScale(4,RoundingMode.HALF_UP).toString()
      )
    })

    import spark.implicits._

    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "root1234")
    MXvalueRDD.toDF().write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://192.168.102.120:3306/JJCWGZ", "sjsmxETL_wmz", properties)
    JGvalueRDD.toDF().write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://192.168.102.120:3306/JJCWGZ", "sjsjgETL_wmz", properties)

    calculator(MXvalueRDD.toDF(),JGvalueRDD.toDF(),spark )

    spark.stop()

  }

  // 初始化变量方法
  private def getRowFieldAsString(row: Row, fieldName: String, defaultValues: String = "0.00"): String = {
    var field = row.getAs[String](fieldName)
    if (field == "") {
      field = defaultValues
    } else if (field == null){
      field = ""
    } else {
      field = field.trim
    }
    field
  }

  //业务代码计算逻辑
  private def calculator(sjsmxDF: DataFrame,sjsjgDF:DataFrame,spark: SparkSession): Unit = {

    val sc = spark.sparkContext
    //将DF转为ARR广播，解决Map内不能使用.rdd方法的问题
    val sjsmxDFArr: Array[Row] = sjsmxDF.collect()
    val sjsmxDFArrBroadCast = sc.broadcast(sjsmxDFArr)
    //将DF转为ARR广播，解决Map内不能使用.rdd方法的问题
    val sjsjgDFArr = sjsjgDF.collect()
    val sjsjgDFArrBroadCast: Broadcast[Array[Row]] = sc.broadcast(sjsjgDFArr)

    sjsmxDF.createOrReplaceTempView("MXTemp")
    val sjsmxTempDF: DataFrame = spark.sqlContext.sql("select MXFJSM,MXYWLB,FJETemp,MXCJRQ from MXTemp")
    val sjsmxTempArrBrodcast = sc.broadcast(sjsmxTempDF.collect())

    sjsjgDF.createOrReplaceTempView("JGTemp")
    val sjsjgTempDF: DataFrame = spark.sqlContext.sql("select JGFJSM,JGYWLB,FJETemp,JGCJRQ from JGTemp")
    val sjsjgTempArrBroadcast = sc.broadcast(sjsjgTempDF.collect())

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
    val csyjlvTableArr: Array[Row] = csyjlvTableDF.rdd.collect()

    val csyjlvTableArrBroadCast = sc.broadcast(csyjlvTableArr)

    //    csyjlvTableDFBroadCast.value.show()

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

      //      val csyjlvTableDFTemp: DataFrame = csyjlvTableDFBroadCast.value
      //      csyjlvTableDFTemp.show()

      // 实收实付金额判断规则
      if ("SFCS".equals(mxywlb) || "SFDQ".equals(mxywlb) || "SFTG".equals(mxywlb) || "SFJZ".equals(mxywlb)) {
        FSSSFJE = BigDecimal(row.getAs[String]("MXSFJE")).abs
      } else if ("SFXZ".equals(mxywlb) && "RZ".equals(FJyFs)) {
        FSSSFJE = MXQSBJ.abs - Fjsf
      } else if ("SFXZ".equals(mxywlb) && "CZ".equals(FJyFs)) {
        FSSSFJE = MXQSBJ.abs + Fjsf
      }

      //日期、成交金额、佣金、回购收益、初始购回期限、融资利率判断规则
      if ("SFCS".equals(mxywlb) || "SFXZ".equals(mxywlb)) {


        println(csyjlvTableArrBroadCast.value.toBuffer)

        val FLVRDD = csyjlvTableArrBroadCast.value.filter(row => {

          FZQLB = row.getAs[String]("FZQLB") //证券类别
          FSZSH = row.getAs[String]("FSZSH") //席位地点
          FSTR1 = row.getAs[String]("FSTR1") //席位号
          "ZQZYSFHG".equals(FZQLB) && "S".equals(FSZSH) && FJyxwh.toString.equals(FSTR1)
        })

        var FLV = "0"
        if (FLVRDD.length == 0) {
          var FLV = "0"
        } else {
          FLV = FLVRDD.head.getAs("FLV")
        }

        FinDate = row.getAs[String]("MXQTRQ") //日期
        Fje = MXQSBJ.abs // 成交金额
        Fyj = Fje.*(Flv).setScale(2, RoundingMode.HALF_UP) // 佣金
        FRZLV = BigDecimal(row.getAs[String]("MXCJJG")) //融资/回购利率
        FHggain = BigDecimal(row.getAs[String]("FHggain")) //回购收益
        FCSGHQX = BigDecimal(row.getAs[String]("FCSGHQXTemp")).setScale(2, RoundingMode.HALF_UP) //初始购回期限

      } else if ("SFDQ".equals(mxywlb) || "SFTG".equals(mxywlb) || "SFJZ".equals(mxywlb)) {

        FinDate = row.getAs[String]("MXCJRQ") //购回日期

        val sjsmxTempArr: Array[Row] = sjsmxTempArrBrodcast.value

        for (i <- sjsmxTempArr) {
          MXFJSM = i(0).toString
          MXYWLB = i(1).toString
          FJETemp = i(2).toString
          if (FCSHTXH.equals(MXFJSM) && ("SFXZ".equals(MXYWLB) || "SFCS".equals(MXYWLB))) {

            Fje = BigDecimal(FJETemp) // 成交/回购金额
            Fyj = BigDecimal(0).setScale(2, RoundingMode.HALF_UP) //拥金
            FHggain = MXQSBJ.abs - Fje //回购收益 是否取绝对值 TODO
            val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

            val strYear1 = i(3).toString.substring(0, 4)
            val strMonth1 = i(3).toString.substring(4, 6)
            val strDay1 = i(3).toString.substring(6, 8)
            val FJEDate = strYear1 + "-" + strMonth1 + "-" + strDay1

            val strYear2 = FinDate.substring(0, 4)
            val strMonth2 = FinDate.substring(4, 6)
            val strDay2 = FinDate.substring(6, 8)
            val FinDateDate = strYear2 + "-" + strMonth2 + "-" + strDay2

            val startDate = simpleDateFormat.parse(FJEDate)
            val endDate = simpleDateFormat.parse(FinDateDate)
            FCSGHQX = (endDate.getTime - startDate.getTime) / (24 * 60 * 60 * 1000) //初始购回期限
            //            FCSGHQX.setScale(2,RoundingMode.HALF_UP)
            FRZLV = BigDecimal(0).setScale(4, RoundingMode.HALF_UP) //融资/回购利率
          }
        }
      }

      SZSETriPartyRepoDto(
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
        FCSGHQX.setScale(2, RoundingMode.HALF_UP).toString(),
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
    Util.outputMySql(sjsmxXzxkRDD.toDF(), "mxfdTest_wmz")

    //    return
    //明细-到期续作前期合约了结数据取值规则
    val sjsmxFilterRDD = sjsmxDF.rdd.filter(row => {
      val MXYWLB: String = row.getAs[String]("MXYWLB")
      "SFXZ".equals(MXYWLB)
    })

    val sjsmxXzljRDD = sjsmxFilterRDD.map(row => {

      FDate = row.getAs[String]("MXCJRQ") //成交日期
      FinDate = row.getAs[String]("MXCJRQ")
      // 初始购回日期
      FJyxwh = row.getAs[String]("MXJYDY") //交易席位号
      FSSSFJE = BigDecimal(row.getAs[String]("MXZJJE")).abs //实收实付金额
      FHTXH = row.getAs[String]("MXYWLSH") //合同序号
      FCSHTXH = row.getAs[String]("MXFJSM") //初始合同序号
      FCSGHQX = BigDecimal(row.getAs[String]("FCSGHQXTemp")) //初始购回期限

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


      val time = DateUtils.formattedDate2Long(FDate, DateUtils.yyyyMMdd)

      val sjsmxSFCSArr: Array[Row] = sjsmxDFArrBroadCast.value.filter(row1 => {

        val ntime = DateUtils.formattedDate2Long(row1.getAs("MXCJRQ"), DateUtils.yyyyMMdd)

        // 过滤掉ETL中自己本身数据，和符"合同初始合同序号"的数据
        (ntime < time) && FCSHTXH.equals(row1.getAs[String]("MXFJSM"))
      })


      val tuples = sjsmxSFCSArr.map(row1 => {
        val Fjea = BigDecimal(row1.getAs[String]("FJETemp"))
        val FHggaina = BigDecimal(row1.getAs[String]("FHggain"))
        val FRZLVa = BigDecimal(row1.getAs[String]("FRZLV"))
        val MXYWLBa= row1.getAs[String]("MXYWLB")
        val FDateTempa = DateUtils.formattedDate2Long(row1.getAs[String]("MXCJRQ"), DateUtils.yyyyMMdd)

        //((FDateTemp, JGYWLB), Fje + "_" + FHggain + "_" + FRZLV + "_" + JGYWLB)
        (MXYWLBa, FDateTempa, Fjea, FHggaina, FRZLVa,row1.getAs[String]("MXCJRQ"))
      })

      /*      for(row1 <- tuples){
              println("==========="+row1)
            }

            println(FDate)*/
      /**
        * 回购金额，回购收益，融资利率
        * 先取MXYWLB为SFXZ的与FDate时间最近一天的数据，取不到再取MXYWLB为SFCS的与FDate最近一天的数据
        */
      val sorted_SFXZ: Array[(Long, BigDecimal, BigDecimal, BigDecimal,String)] = sjsmxSFCSArr.filter(item => {
        "SFXZ".equals(item.getAs("MXYWLB"))
      }).map(item => {
        val Fje_SFXZ = BigDecimal(item.getAs[String]("FJETemp"))
        val FHggain_SFXZ = BigDecimal(item.getAs[String]("FHggain"))
        val FRZLV_SFXZ = BigDecimal(item.getAs[String]("FRZLV"))
        val FDateTemp_SFXZ = DateUtils.formattedDate2Long(item.getAs[String]("MXCJRQ"), DateUtils.yyyyMMdd)
        (FDateTemp_SFXZ, Fje_SFXZ, FHggain_SFXZ, FRZLV_SFXZ,item.getAs[String]("MXCJRQ"))
      }).sortBy(item => item._1)

      if (sorted_SFXZ.length > 0) {
        val head: (Long, BigDecimal, BigDecimal, BigDecimal,String) = sorted_SFXZ.reverse.head
        Fje = head._2
        FHggain = head._3
        FRZLV = head._4
      } else {

        val sorted_SFCS: Array[(Long, BigDecimal, BigDecimal, BigDecimal,String)] = sjsmxSFCSArr.filter(item => {
          "SFCS".equals(item.getAs("MXYWLB"))
        }).map(item => {
          val Fje_SFCS = BigDecimal(item.getAs[String]("FJETemp"))
          val FHggain_SFCS = BigDecimal(item.getAs[String]("FHggain"))
          val FRZLV_SFCS = BigDecimal(item.getAs[String]("FRZLV"))
          val FDateTemp_SFCS = DateUtils.formattedDate2Long(item.getAs[String]("MXCJRQ"), DateUtils.yyyyMMdd)
          (FDateTemp_SFCS, Fje_SFCS, FHggain_SFCS, FRZLV_SFCS,item.getAs[String]("MXCJRQ"))
        }).sortBy(item => item._1)

        if (sorted_SFCS.length > 0) {
          val head = sorted_SFCS.reverse.head
          Fje = head._2
          FHggain = head._3
          FRZLV = head._4
        }

      }

      var FJyFs: String = new String

      if (BigDecimal(row.getAs[String]("MXQSBJ")) < 0) {
        FJyFs = "RZ" //交易方式
      } else if (BigDecimal(row.getAs[String]("MXQSBJ")) > 0) {
        FJyFs = "CZ" //交易方式
      }

      SZSETriPartyRepoDto(
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
      if ("SFCS".equals(jgywlb) || "SFDQ".equals(jgywlb) || "SFTG".equals(jgywlb) || "SFJZ".equals(jgywlb)) {
        FSSSFJE = BigDecimal(row.getAs[String]("JGSFJE")).abs
      } else if ("SFXZ".equals(jgywlb) && "RZ".equals(FJyFs)) {
        FSSSFJE = JGQSBJ.abs - Fjsf
      } else if ("SFXZ".equals(jgywlb) && "CZ".equals(FJyFs)) {
        FSSSFJE = JGQSBJ.abs + Fjsf
      }

      //日期、成交金额、佣金、回购收益、初始购回期限、融资利率判断规则
      if ("SFCS".equals(jgywlb) || "SFXZ".equals(jgywlb)) {

        val FLVRDD = csyjlvTableArrBroadCast.value.filter(row => {
          FZQLB = row.getAs[String]("FZQLB") //证券类别
          FSZSH = row.getAs[String]("FSZSH") //席位地点
          FSTR1 = row.getAs[String]("FSTR1") //席位号
          "ZQZYSFHG".equals(FZQLB) && "S".equals(FSZSH) && FJyxwh.equals(FSTR1)
        })

        var FLV = "0"
        if (FLVRDD.length == 0) {
          var FLV = "0"
        } else {
          FLV = FLVRDD.head.getAs("FLV")
        }

        // 佣金费率

        FinDate = row.getAs[String]("JGQTRQ") //日期
        Fje = JGQSBJ.abs // 成交金额
        Fyj = Fje.*(Flv).setScale(2, RoundingMode.HALF_UP) // 佣金
        FRZLV = BigDecimal(row.getAs[String]("JGCJJG")) //融资/回购利率
        FHggain = BigDecimal(row.getAs[String]("FHggain")) //回购收益
        FCSGHQX = BigDecimal(row.getAs[String]("FCSGHQXTemp")) //初始购回期限

      } else if ("SFDQ".equals(jgywlb) || "SFTG".equals(jgywlb) || "SFJZ".equals(jgywlb)) {
        FinDate = row.getAs[String]("JGCJRQ") //购回日期

        //获取对应初试合同序号的业务类别为SFXZ的回购业务
        val FjeArr = sjsjgTempArrBroadcast.value.filter(row => {
          JGFJSM = row.getAs[String]("JGFJSM")
          JGYWLB = row.getAs[String]("JGYWLB")
          FJETemp = row.getAs[String]("FJETemp")

          FCSHTXH.equals(JGFJSM) && "SFXZ".equals(JGYWLB) || "SFCS".equals(JGYWLB)
        })

        //获取广播变量
        val sjsjgTempArr: Array[Row] = sjsjgTempArrBroadcast.value

        for (i <- sjsjgTempArr) {

          JGFJSM = i(0).toString
          JGYWLB = i(1).toString
          FJETemp = i(2).toString
          if (FCSHTXH.equals(JGFJSM) && ("SFXZ".equals(JGYWLB) || "SFCS".equals(JGYWLB))) {

            Fje = BigDecimal(FJETemp) // 成交/回购金额
            Fyj = BigDecimal(0).setScale(2, RoundingMode.HALF_UP) //拥金
            FHggain = JGQSBJ.abs - Fje //回购收益 是否取绝对值 TODO

            val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

            val strYear1 = i(3).toString.substring(0, 4)
            val strMonth1 = i(3).toString.substring(4, 6)
            val strDay1 = i(3).toString.substring(6, 8)
            val FJEDate = strYear1 + "-" + strMonth1 + "-" + strDay1

            val strYear2 = FinDate.substring(0, 4)
            val strMonth2 = FinDate.substring(4, 6)
            val strDay2 = FinDate.substring(6, 8)
            val FinDateDate = strYear2 + "-" + strMonth2 + "-" + strDay2

            val startDate = simpleDateFormat.parse(FJEDate)
            val endDate = simpleDateFormat.parse(FinDateDate)
            FCSGHQX = (endDate.getTime - startDate.getTime) / (24 * 60 * 60 * 1000) //初始购回期限
            FRZLV = BigDecimal(0).setScale(2, RoundingMode.HALF_UP) //融资/回购利率
          }
        }

        FRZLV = BigDecimal(0) //融资/回购利率
      }

      SZSETriPartyRepoDto(
        FDate,
        FinDate,
        FZqdm,
        FSzsh,
        FJyxwh,
        Fje.toString(),
        Fyj.toString(),
        Fjsf.toString(),
        FHggain.toString(),
        FSSSFJE.setScale(2, RoundingMode.HALF_UP).toString(),
        FZqbz,
        Fjybz,
        FJyFs,
        Fsh,
        Fzzr,
        Fchk,
        FHTXH,
        FSETCODE.toString(),
        FCSGHQX.setScale(2, RoundingMode.HALF_UP).toString(),
        FRZLV.setScale(4, RoundingMode.HALF_UP).toString(),
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
      FCSGHQX = BigDecimal(row.getAs[String]("FCSGHQXTemp")) //初始购回期限


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

      val time = DateUtils.formattedDate2Long(FDate, DateUtils.yyyyMMdd)

      val sjsjgSFCSArr: Array[Row] = sjsjgDFArrBroadCast.value.filter(row1 => {

        val ntime = DateUtils.formattedDate2Long(row1.getAs("JGCJRQ"), DateUtils.yyyyMMdd)

        (ntime < time) && FCSHTXH.equals(row1.getAs[String]("JGFJSM"))
      })

      /* var FDateTemp = BigDecimal(0)
       val SFCSvalue = sjsjgSFCSArr.map(row => {
         Fje = BigDecimal(row.getAs[String]("FJETemp"))
         FHggain = BigDecimal(row.getAs[String]("FHggain"))
         FRZLV = BigDecimal(row.getAs[String]("FRZLV"))
         JGYWLB = row.getAs("JGYWLB")
         FDateTemp = BigDecimal(row.getAs[String]("JGCJRQ"))

         ((FDateTemp, JGYWLB), Fje + "_" + FHggain + "_" + FRZLV + "_" + JGYWLB)
       })


       val SFCSvalueTup: Map[(BigDecimal, String), Array[((BigDecimal, String), String)]] = SFCSvalue.groupBy(tup => tup._1)*/



      val tuples = sjsjgSFCSArr.map(row1 => {
        val Fjea = BigDecimal(row1.getAs[String]("FJETemp"))
        val FHggaina = BigDecimal(row1.getAs[String]("FHggain"))
        val FRZLVa = BigDecimal(row1.getAs[String]("FRZLV"))
        val JGYWLBa= row1.getAs[String]("JGYWLB")
        val FDateTempa = DateUtils.formattedDate2Long(row1.getAs[String]("JGCJRQ"), DateUtils.yyyyMMdd)

        //((FDateTemp, JGYWLB), Fje + "_" + FHggain + "_" + FRZLV + "_" + JGYWLB)
        (JGYWLBa, FDateTempa, Fjea, FHggaina, FRZLVa,row1.getAs[String]("JGCJRQ"))
      })

      /*      for(row1 <- tuples){
              println("==========="+row1)
            }

            println(FDate)*/

      /**
        * 回购金额，回购收益，融资利率
        * 先取MXYWLB为SFXZ的与FDate时间最近一天的数据，取不到再取MXYWLB为SFCS的与FDate最近一天的数据
        */

      val sorted_SFXZ: Array[(Long, BigDecimal, BigDecimal, BigDecimal,String)] = sjsjgSFCSArr.filter(item => {
        "SFXZ".equals(item.getAs("JGYWLB"))
      }).map(item => {
        val Fje_SFXZ = BigDecimal(item.getAs[String]("FJETemp"))
        val FHggain_SFXZ = BigDecimal(item.getAs[String]("FHggain"))
        val FRZLV_SFXZ = BigDecimal(item.getAs[String]("FRZLV"))
        val FDateTemp_SFXZ = DateUtils.formattedDate2Long(item.getAs[String]("JGCJRQ"), DateUtils.yyyyMMdd)
        (FDateTemp_SFXZ, Fje_SFXZ, FHggain_SFXZ, FRZLV_SFXZ,item.getAs[String]("JGCJRQ"))
      }).sortBy(item => item._1)

      if (sorted_SFXZ.length > 0) {
        val head: (Long, BigDecimal, BigDecimal, BigDecimal,String) = sorted_SFXZ.reverse.head
        Fje = head._2
        FHggain = head._3
        FRZLV = head._4
      } else {

        val sorted_SFCS: Array[(Long, BigDecimal, BigDecimal, BigDecimal,String)] = sjsjgSFCSArr.filter(item => {
          "SFCS".equals(item.getAs("JGYWLB"))
        }).map(item => {
          val Fje_SFCS = BigDecimal(item.getAs[String]("FJETemp"))
          val FHggain_SFCS = BigDecimal(item.getAs[String]("FHggain"))
          val FRZLV_SFCS = BigDecimal(item.getAs[String]("FRZLV"))
          val FDateTemp_SFCS = DateUtils.formattedDate2Long(item.getAs[String]("JGCJRQ"), DateUtils.yyyyMMdd)
          (FDateTemp_SFCS, Fje_SFCS, FHggain_SFCS, FRZLV_SFCS,item.getAs[String]("JGCJRQ"))
        }).sortBy(item => item._1)

        if (sorted_SFCS.length > 0) {
          val head = sorted_SFCS.reverse.head
          Fje = head._2
          FHggain = head._3
          FRZLV = head._4
        }

      }


      /*


      import scala.util.control.Breaks._

      for (i <- SFCSValueList._2) {
        sjsjgSFCS.appendAll(i._2.split("_"))
        breakable {

          if ("SFXZ".equals(sjsjgSFCS(3))) {
            Fje = BigDecimal(sjsjgSFCS(0)) //成交金额
                        break()
          }
          else if ("SFCS".equals(sjsjgSFCS(3))) {

            Fje = BigDecimal(sjsjgSFCS(0)) //成交金额

          }
        }
      }

            val SFCSvalue1 = sjsjgSFCSArr.map(row => {
              FHggain = BigDecimal(row.getAs[String]("FHggain"))
              FRZLV = BigDecimal(row.getAs[String]("FRZLV"))
              JGYWLB = row.getAs("JGYWLB")
              FHggain + "_" + FRZLV + "_" + JGYWLB
            })

            var sjsjgSFCS1: ArrayBuffer[String] = new ArrayBuffer[String]

            import scala.util.control.Breaks._
            breakable {
              for (i <- SFCSvalue1) {
                sjsjgSFCS1.appendAll(i.split("_"))
                if ("SFXZ".equals(sjsjgSFCS1(2))) {
                  FHggain = BigDecimal(sjsjgSFCS1(0)) //回购收益
                  FRZLV = BigDecimal(sjsjgSFCS1(1)) //融资/购回利率
                  FCSGHQX = BigDecimal(row.getAs[String]("FCSGHQXTemp")) //初始购回期限

                  break()
                }
                else if ("SFCS".equals(sjsjgSFCS1(2))) {
                  FHggain = BigDecimal(sjsjgSFCS1(0)) //回购收益
                  FRZLV = BigDecimal(sjsjgSFCS1(1)) //融资/购回利率
                  FCSGHQX = BigDecimal(row.getAs[String]("FCSGHQXTemp")) //初始购回期限
                }
              }
            }
*/

      var FJyFs: String = ""

      if (BigDecimal(row.getAs[String]("JGQSBJ")) < 0) {
        FJyFs = "RZ" //交易方式
      } else if (BigDecimal(row.getAs[String]("JGQSBJ")) > 0) {
        FJyFs = "CZ" //交易方式
      }

      SZSETriPartyRepoDto(
        FDate,
        FinDate,
        FZqdm,
        FSzsh,
        FJyxwh,
        Fje.toString(),
        Fyj.toString(),
        Fjsf.toString(),
        FHggain.toString(),
        FSSSFJE.setScale(2, RoundingMode.HALF_UP).toString(),
        FZqbz,
        Fjybz,
        FJyFs,
        Fsh,
        Fzzr,
        Fchk,
        FHTXH,
        FSETCODE.toString(),
        FCSGHQX.setScale(2, RoundingMode.HALF_UP).toString(),
        FRZLV.setScale(4, RoundingMode.HALF_UP).toString(),
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



