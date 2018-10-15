package com.yss.scala.guzhi

import java.text.SimpleDateFormat
import java.util.Properties

import com.yss.scala.dbf.dbf._
import com.yss.scala.dto.{JGETL, MXETL}
import com.yss.scala.util.DateUtils
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
object SZSETriPartyRepoETL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SHDZGH").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    //原始数据
    val sjsjg: DataFrame = spark.sqlContext.dbfFile("hdfs://192.168.102.120:8020/tmp/SFHGTest/SJSJG/sjsjg0723.dbf")
    val sjsmx: DataFrame = spark.sqlContext.dbfFile("hdfs://192.168.102.120:8020/tmp/SFHGTest/sjsmx5.dbf")

    val sjsmxFilterRDD = sjsmx.rdd.filter(row => {
      "20180723".equals(row.getAs[String]("MXCJRQ")) &&
        "SFCS SFDQ SFXZ SFTG SFJZ".contains(row.getAs[String]("MXYWLB")) &&
        "02".equals(row.getAs[String]("MXJYFS")) &&
        "01".equals(row.getAs[String]("MXSJLX"))
    })

    val sjsjgFilterRDD = sjsjg.rdd.filter(row => {
      "20180723".equals(row.getAs[String]("JGCJRQ")) &&
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
        FRZLV.setScale(4,RoundingMode.HALF_UP).toString(),
        FHggain.setScale(2,RoundingMode.HALF_UP).toString()
      )
    })

    import spark.implicits._

    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "root1234")
    MXvalueRDD.toDF().write.mode(SaveMode.Append).jdbc("jdbc:mysql://192.168.102.120:3306/JJCWGZ", "sjsmxETL_wmz", properties)
    JGvalueRDD.toDF().write.mode(SaveMode.Append).jdbc("jdbc:mysql://192.168.102.120:3306/JJCWGZ", "sjsjgETL_wmz", properties)

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

}

