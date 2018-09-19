package com.yss.scala.guzhi

import com.yss.scala.util.{DateUtils, Util}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

/**
  * @auther: lijiayan
  * @date: 2018/9/17
  * @desc: 固定收益平台三方回购业务
  *        源文件:jsmx和wdq清洗后的数据
  *        目标表:
  */
object GuDingShouYiSFHG {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName(getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    // 读取etl后的csv文件并转化成RDD
    val path = "hdfs://192.168.13.110:9000/guzhi/etl/sfgu/20180918/"
    val sfhgDataRDD = Util.readCSV(path, spark).rdd.map(row => {
      val xwh = row.getAs[String]("XWH1").trim
      (xwh, row)
      /*("259700", row)*/
    })

    // 读取csqsxw,并得到<席位号,套账号>的RDD
    val xwhAndTzhRDD: RDD[(String, String)] = readCSQSXW(spark)

    //得到<席位号,<dataRow,套账号>>
    val xwh2DataAndTZH: RDD[(String, (Row, String))] = sfhgDataRDD.join(xwhAndTzhRDD)

    //将xwh2DataAndTZH 转化成 (套账号+"交易所回购计算佣金",(dataRow,套账号))
    val tzh_selectItemName2DataRowAndTzh: RDD[(String, (Row, String))] = xwh2DataAndTZH.map(item => {
      (item._2._2 + "交易所回购计算佣金", item._2)
    })

    //读取参数表
    val tzh_selectItemName2SelectResult: RDD[(String, Boolean)] = readLVARLIST(spark)

    // <数据,套账号,选项结果>
    val rowDataAndTzhAndSelected: RDD[(Row, String, Boolean)] = tzh_selectItemName2DataRowAndTzh.join(tzh_selectItemName2SelectResult).map(item => {
      (item._2._1._1, item._2._1._2, item._2._2)
    })

    //读取佣金表,得到<证券类别|席位号|市场号,佣金利率>
    val zqlbAndXwhAndSC2YjFV: RDD[(String, String)] = readA117CSYJLV(spark)

    //<证券类别|席位号|市场号,(数据,套账号,选项结果)>
    val zqlbAndXwhAndSC2RowDataAndTzhAndSelected: RDD[(String, (Row, String, Boolean))] = rowDataAndTzhAndSelected.map(item => {
      val xwh = item._1.getAs[String]("XWH1").trim
      ("ZQZYSFHG|" + xwh + "|G", item)
      /*("GP|" + "000001" + "|S", item)*/
    })

    //<数据,套账号,选项结果,佣金利率>
    val rowDataAndTzhAndSelectedAndYjFV: RDD[(Row, String, Boolean, String)] = zqlbAndXwhAndSC2RowDataAndTzhAndSelected.join(zqlbAndXwhAndSC2YjFV).map(item => {
      (item._2._1._1, item._2._1._2, item._2._1._3, item._2._2)
    })

    //开始计算
    val resSFHGRDD: RDD[SFHG] = calculate(rowDataAndTzhAndSelectedAndYjFV)
    resSFHGRDD.collect().foreach(println(_))

    spark.stop()


  }

  case class SFHG(
                   FDate: String,
                   FInDate: String,
                   FZqdm: String,
                   FSzsh: String,
                   FJyxwh: String,
                   Fje: String,
                   Fyj: String,
                   Fjsf: String,
                   FHggain: String,
                   FSSSFJE: String,
                   FZqbz: String,
                   Fjybz: String,
                   ZqDm: String,
                   FJyFs: String,
                   Fsh: String,
                   Fzzr: String,
                   Fchk: String,
                   FHTXH: String,
                   FSETCODE: String,
                   FCSGHQX: String,
                   FRZLV: String,
                   FSJLY: String,
                   FCSHTXH: String,
                   FBS: String,
                   FSL: String,
                   Fyhs: String,
                   Fzgf: String,
                   Fghf: String,
                   FFxj: String,
                   FQtf: String,
                   Fgzlx: String,
                   FQsbz: String,
                   ftzbz: String,
                   FQsghf: String,
                   FGddm: String,
                   fzlh: String,
                   ISRTGS: String,
                   FPARTID: String,
                   FYwbz: String,
                   Fbz: String
                 )


  /**
    * 开始计算
    *
    * @param rowDataAndTzhAndSelectedAndYjFV opp RDD
    * @return
    *
    */
  def calculate(rowDataAndTzhAndSelectedAndYjFV: RDD[(Row, String, Boolean, String)]): RDD[GuDingShouYiSFHG.SFHG] = {
    rowDataAndTzhAndSelectedAndYjFV.map(item => {
      val row = item._1
      val tzh = item._2
      val isSelected = item._3
      val fv = item._4

      val FDate = row.getAs[String]("JYRQ").trim

      val FZqdm = " "

      val FSzsh = "G"

      val FJyxwh = row.getAs[String]("XWH1").trim

      val FZqbz = row.getAs[String]("FZQBZ").trim

      val Fjybz = row.getAs[String]("FJYBZ").trim

      val ZqDm = row.getAs[String]("ZQDM1").trim

      val MMBZ = row.getAs[String]("MMBZ").trim
      var FJyFs = " "
      if ("B".equals(MMBZ)) {
        FJyFs = "RZ"
      } else if ("S".equals(MMBZ)) {
        FJyFs = "CZ"
      }

      val Fsh = "1"

      val Fzzr = "admin"

      val Fchk = "admin"

      val FHTXH = row.getAs[String]("CJBH").trim

      val FSETCODE = tzh

      val FRZLV = BigDecimal(row.getAs[String]("JG1").trim)

      val FSJLY = "ZD"

      val FBS = MMBZ

      val FSL = BigDecimal(0.00)

      val Fyhs = BigDecimal(row.getAs[String]("YHS").trim)

      val Fzgf = BigDecimal(row.getAs[String]("ZGF").trim)

      val Fghf = BigDecimal(row.getAs[String]("GHF").trim)

      val FFxj = BigDecimal(0.00)

      val FQtf = BigDecimal(0.00)


      val Fgzlx = BigDecimal(0.00)

      val FQsbz = " "

      val ftzbz = " "

      val FQsghf = BigDecimal(0.00)

      val FGddm = " "

      val fzlh = " "

      val ISRTGS = " "

      val FPARTID = " "

      val FYwbz = " "

      val Fbz = " "

      //qtrq-cjrq
      val QTRQCJRQ = row.getAs[String]("QTRQCJRQ")

      val YWLX = row.getAs[String]("YWLX").trim

      //=====================上面是公有变量,下面是根据业务类型变化的变量计算================================
      var FInDate = ""

      var Fje = BigDecimal(0.00)

      var Fyj = BigDecimal(0.00)

      var Fjsf = BigDecimal(row.getAs[String]("JSF").trim).abs

      //初始回购期限
      var FCSGHQX = BigDecimal(0.00)

      var FCSHTXH = ""

      var FSSSFJE = BigDecimal(0.00)

      if ("680".equals(YWLX)) {
        FInDate = row.getAs[String]("QTRQ").trim
        Fje = BigDecimal(row.getAs[String]("QSJE").trim).abs
        FCSGHQX = BigDecimal(DateUtils.absDays(FInDate, FDate))
        Fyj = Fje * BigDecimal(fv)
        FSSSFJE = BigDecimal(row.getAs[String]("SJSF").trim).abs
        FCSHTXH = row.getAs[String]("CJBH").trim
      }
      else if ("681".equals(YWLX)) {
        FInDate = row.getAs[String]("JYRQ").trim
        FCSGHQX = BigDecimal(DateUtils.absDays(FInDate, row.getAs[String]("QTRQ").trim))
        Fje = BigDecimal(row.getAs[String]("QSJE").trim).abs / (1 + BigDecimal(row.getAs[String]("JG1").trim) / 100 * FCSGHQX / 365)
        FSSSFJE = BigDecimal(row.getAs[String]("SJSF").trim).abs
        FCSHTXH = row.getAs[String]("SQBH").trim
      }
      else if ("683".equals(YWLX)) {
        FInDate = row.getAs[String]("JYRQ").trim
        FCSGHQX = BigDecimal(DateUtils.absDays(FInDate, row.getAs[String]("QTRQ").trim))
        Fje = BigDecimal(row.getAs[String]("QSJE").trim).abs / (1 + FRZLV / 100 * FCSGHQX / 365)
        FSSSFJE = BigDecimal(row.getAs[String]("SJSF").trim).abs
        FCSHTXH = row.getAs[String]("SQBH").trim
      }
      //682
      else {
        FCSHTXH = row.getAs[String]("SQBH").trim
        //682续作合约新开数据取值规则
        if ("XZXK_SFHG".equals(Fjybz)) {
          FInDate = DateUtils.addDays(FDate, QTRQCJRQ.toInt)
          FCSGHQX = BigDecimal(QTRQCJRQ)
          Fje = BigDecimal(row.getAs[String]("QSJE").trim).abs

          if (isSelected) {
            Fyj = Fje * BigDecimal(fv)
          }
          FSSSFJE = BigDecimal(row.getAs[String]("QSJE").trim).abs

        }
        //682续作前期合约了结数据取值规则
        else {
          FInDate = FDate
          FCSGHQX = BigDecimal(QTRQCJRQ)
          Fje = BigDecimal(row.getAs[String]("QTJE1").trim).abs / (1 + FRZLV.setScale(4, BigDecimal.RoundingMode.HALF_UP) / 100 * FCSGHQX / 365)
          Fyj = BigDecimal(0.00).setScale(2, BigDecimal.RoundingMode.HALF_UP)
          Fjsf = BigDecimal(0.00).setScale(2, BigDecimal.RoundingMode.HALF_UP)
          FSSSFJE = BigDecimal(row.getAs[String]("QTJE1").trim).setScale(2, BigDecimal.RoundingMode.HALF_UP)
        }

      }

      val FHggain = Fje * FRZLV / (100 * FCSGHQX / 365)

      SFHG(
        FDate,
        FInDate,
        FZqdm,
        FSzsh,
        FJyxwh,
        Fje.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString(),
        Fyj.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString(),
        Fjsf.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString(),
        FHggain.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString(),
        FSSSFJE.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString(),
        FZqbz,
        Fjybz,
        ZqDm,
        FJyFs,
        Fsh,
        Fzzr,
        Fchk,
        FHTXH,
        FSETCODE,
        FCSGHQX.setScale(0, BigDecimal.RoundingMode.HALF_UP).toString(),
        FRZLV.setScale(4, BigDecimal.RoundingMode.HALF_UP).toString(),
        FSJLY,
        FCSHTXH,
        FBS,
        FSL.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString(),
        Fyhs.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString(),
        Fzgf.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString(),
        Fghf.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString(),
        FFxj.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString(),
        FQtf.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString(),
        Fgzlx.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString(),
        FQsbz,
        ftzbz,
        FQsghf.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString(),
        FGddm,
        fzlh,
        ISRTGS,
        FPARTID,
        FYwbz,
        Fbz
      )

    })
  }

  /**
    * 读取佣金利率表 A117CSYJLV
    *
    * @param spark SparkSession
    * @return <证券类别|席位号|市场号,佣金>
    */
  def readA117CSYJLV(spark: SparkSession): RDD[(String, String)] = {
    Util.readCSV(getTableDataPath("A117CSYJLV"), spark, header = false).toDF(
      "FID",
      "FZQLB",
      "FSZSH",
      "FLV",
      "FLVMIN",
      "FTJ1",
      "FSTR1",
      "FTJ2",
      "FTJ2FROM",
      "FTJ2TO",
      "FLVZK",
      "FSH",
      "FZZR",
      "FCHK",
      "FSTARTDATE",
      "FJJDM",
      "FGDJE"
    ).rdd.map(row => {
      val FZQLB = row.getAs[String]("FZQLB").trim
      val FSZSH = row.getAs[String]("FSZSH").trim
      val FSTR1 = row.getAs[String]("FSTR1").trim
      val FLV = row.getAs[String]("FLV").trim
      (FZQLB + "|" + FSTR1 + "|" + FSZSH, FLV)
    })
  }


  /**
    * 读取参数表 lvarlist  参数表
    *
    * @param spark SparkSession
    * @return <选项名称,是否选中(true/false)>
    */
  def readLVARLIST(spark: SparkSession): RDD[(String, Boolean)] = {
    Util.readCSV(getTableDataPath("LVARLIST"), spark, header = false).toDF(
      "FVARNAME",
      "FVARVALUE",
      "FSH",
      "FZZR",
      "FCHK",
      "FSTARTDATE"
    ).rdd.map(row => {
      val FVARNAME = row.getAs[String]("FVARNAME")
      val FVARVALUE = row.getAs[String]("FVARVALUE")

      var checked = false
      if ("1".equals(FVARVALUE)) checked = true
      (FVARNAME, checked)
    })
  }


  /**
    * 读取 csqsxw    席位表
    *
    * @param spark SparkSession
    * @return 返回<席位号,套账号>
    */
  def readCSQSXW(spark: SparkSession): RDD[(String, String)] = {
    Util.readCSV(getTableDataPath("CSQSXW"), spark, header = false).toDF(
      "FQSDM",
      "FQSMC",
      "FSZSH",
      "FQSXW",
      "FXWLB",
      "FSETCODE",
      "FSH",
      "FZZR",
      "FCHK",
      "FSTARTDATE"
    ).rdd.map(row => {
      val xwh = row.getAs[String]("FQSXW").trim
      val tzh = row.getAs[String]("FSETCODE").trim
      val FSTARTDATE = row.getAs[String]("FSTARTDATE").trim
      (xwh, (tzh, DateUtils.formattedDate2Long(FSTARTDATE, DateUtils.yyyy_MM_dd)))
    }).groupByKey().map(item => {
      //由于席位号和套账号相同的情况会有多个,得根据日期来取最大值
      (item._1, item._2.toList.sortBy(tup => tup._2).reverse.head)
    }).map(item => {
      (item._1, item._2._1)
    })
  }


  /**
    * 根据表名获取表在hdfs上对应的路径
    *
    * @param tName :表面
    * @return
    */
  def getTableDataPath(tName: String): String = {
    val date = DateUtils.formatDate(System.currentTimeMillis())
    "hdfs://192.168.102.120:8020/yss/guzhi/basic_list/" + date + "/" + tName
  }
}
