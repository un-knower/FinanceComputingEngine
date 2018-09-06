package com.yss.scala.util

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
  * @author yupan
  *         2018/8/7 9:48
  **/
object execution_aggr {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("yss").getOrCreate()
    val sc = spark.sparkContext

    val input ="C:\\Users\\yupan\\Desktop\\20180810\\execution_aggr_F000995F0401_1_20180808.tsv"
    val output = "C:\\Users\\yupan\\Desktop\\execution_aggr"

    val path = new Path(output)
    val conf = new Configuration()
    path.getFileSystem(conf).delete(path,true)

    val datetmp = new Date()
    val sm = new SimpleDateFormat("yyyyMMdd")
    val date = sm.format(datetmp)

    val yinhuashui =0.001
    val guohufei =0.00004
    val jingshoufei = 0.000148
    val zhenguanfei = 0.00004
    val fengxianjin = 0.00004
    val yongjinlilv = 0.0025


    val value_tmp = sc.textFile(input).map(f=>{
      val value_split=f.split("\t")

      val Fdate = date
      val FinDate =date
      val FZqdm = value_split(5)
      val FSzsh ="s"
      val Fjyxwh=value_split(3)

      var Fbje =""
      var Fsje =""
      var FBsl =""
      var FSsl =""
      var Fbyj =""
      var Fsyj =""
      var FBjsf =""
      var FSjsf =""
      var Fbyhs =""
      var Fsyhs =""
      var FBzgf =""
      var FSzgf =""
      var FBghf =""
      var FSghf =""

      val FBgzlx ="0"
      val FSgzlx ="0"
      var FBfxj =""
      var FSfxj =""
      var Fbsfje =""
      var Fsssje =""

      val FZqbz ="GP"
      val Fywbz ="PT"
      val FQsbz ="N"
      val FBQTF ="0"
      val FSQTF ="0"
      val ZQDM=value_split(5)
      val FJYFS ="PT"
      val Fsh ="1"
      val FZZR =""
      val FCHK =""
      val fzlh ="0"
      val ftzbz =""
      val FBQsghf ="0"
      val FsQsghf ="0"
      val fgddm =value_split(21)
        if(value_split(20)=="1"){
          Fbje=(BigDecimal(value_split(16))*BigDecimal(value_split(17))).toString()
          Fsje="0"
          FBsl=value_split(17)
          FSsl="0"

          FBjsf=(BigDecimal(Fbje)*BigDecimal(jingshoufei)).toString
          FSjsf="0"
//          Fbyhs=(BigDecimal(Fbje)*BigDecimal(yinhuashui)).toString()
          Fbyhs="0"
          Fsyhs="0"
          FBzgf=(BigDecimal(Fbje)*BigDecimal(zhenguanfei)).toString()
          FSzgf="0"
          FBghf=(BigDecimal(Fbje)*BigDecimal(guohufei)).toString()
          FSghf="0"

          FBfxj=(BigDecimal(Fbje)*BigDecimal(fengxianjin)).toString()
          FSfxj="0"
          Fbsfje=(BigDecimal(Fbje)+BigDecimal(FBjsf)+BigDecimal(FBzgf)+BigDecimal(FBghf)).toString()
          Fsssje=(BigDecimal(Fbje)+BigDecimal(FBjsf)+BigDecimal(FBzgf)+BigDecimal(FBghf)).toString()

          Fbyj=(BigDecimal(Fbje)*BigDecimal(yongjinlilv)-BigDecimal(FBzgf)-BigDecimal(FBghf)-BigDecimal(Fbyhs)).toString()
          Fsyj="0"
        }
      if(value_split(20)=="2"){
          Fbje="0"
          Fsje=(BigDecimal(value_split(16))*BigDecimal(value_split(17))).toString()
          FBsl="0"
          FSsl=value_split(17)

          FBjsf="0"
          FSjsf=(BigDecimal(Fsje)*BigDecimal(jingshoufei)).toString()
          Fbyhs="0"
          Fsyhs=(BigDecimal(Fsje)*BigDecimal(yinhuashui)).toString()
          FBzgf="0"
          FSzgf=(BigDecimal(Fsje)*BigDecimal(zhenguanfei)).toString()
          FBghf="0"
          FSghf=(BigDecimal(Fsje)*BigDecimal(guohufei)).toString()

          FBfxj="0"
          FSfxj=(BigDecimal(Fsje)*BigDecimal(fengxianjin)).toString()
          Fbsfje=(BigDecimal(Fbje)+BigDecimal(FBjsf)+BigDecimal(FBzgf)+BigDecimal(FBghf)).toString()
          Fsssje=(BigDecimal(Fbje)+BigDecimal(FBjsf)+BigDecimal(FBzgf)+BigDecimal(FBghf)).toString()

          Fbyj="0"
          Fsyj=(BigDecimal(Fsje)*BigDecimal(yongjinlilv)-BigDecimal(FSzgf)-BigDecimal(FSghf)-BigDecimal(Fsyhs)).toString
      }

      //日期 +SecurityID +市场(S)+交易席位(ReportingPBUID)+SIDE(买卖)+股东代码(AccountID)
//      val key = date+","+value_split(5)+","+FSzsh+","+value_split(3)+","+value_split(20)+","+value_split(21)
      val key = date+","+value_split(5)+","+FSzsh+","+value_split(3)+","+value_split(20)
      val valuefinal =List(Fdate,FinDate,FZqdm,FSzsh,Fjyxwh,Fbje,Fsje,FBsl,FSsl,Fbyj,Fsyj,FBjsf,FSjsf,Fbyhs,Fsyhs,FBzgf,FSzgf,FBghf,FSghf,FBgzlx,FSgzlx,FBfxj,FSfxj,Fbsfje,Fsssje,FZqbz,Fywbz,FQsbz,FBQTF,FSQTF,ZQDM,FJYFS,Fsh,FZZR,FCHK,fzlh,ftzbz,FBQsghf,FsQsghf,fgddm)
      (key,valuefinal)
    })

    val value_final = value_tmp.groupByKey().map(f=>{
      val firstvalue = f._2.toList(0)
      val Fdate = firstvalue(0)
      val FinDate =firstvalue(1)
      val FZqdm = firstvalue(2)
      val FSzsh = firstvalue(3)
      val Fjyxwh= firstvalue(4)

      var Fbje_sum = BigDecimal(0.0)
      var Fsje_sum = BigDecimal(0.0)
      var FBsl_sum = BigDecimal(0.0)
      var FSsl_sum = BigDecimal(0.0)
      var Fbyj_sum = BigDecimal(0.0)
      var Fsyj_sum = BigDecimal(0.0)
      var FBjsf_sum = BigDecimal(0.0)
      var FSjsf_sum = BigDecimal(0.0)
      var Fbyhs_sum = BigDecimal(0.0)
      var Fsyhs_sum = BigDecimal(0.0)
      var FBzgf_sum = BigDecimal(0.0)
      var FSzgf_sum = BigDecimal(0.0)
      var FBghf_sum = BigDecimal(0.0)
      var FSghf_sum = BigDecimal(0.0)

      var FBgzlx = firstvalue(19)
      var FSgzlx = firstvalue(20)

      var FBfxj_sum = BigDecimal(0.0)
      var FSfxj_sum = BigDecimal(0.0)
      var Fbsfje_sum = BigDecimal(0.0)
      var Fsssje_sum = BigDecimal(0.0)


      var FZqbz = firstvalue(25)
      var Fywbz = firstvalue(26)
      var FQsbz = firstvalue(27)
      var FBQTF = firstvalue(28)
      var FSQTF = firstvalue(29)
      var ZQDM = firstvalue(30)
      var FJYFS = firstvalue(31)
      var Fsh = firstvalue(32)
      var FZZR = firstvalue(33)
      var FCHK = firstvalue(34)
      var fzlh = firstvalue(35)
      var ftzbz = firstvalue(36)
      var FBQsghf = firstvalue(37)
      var FsQsghf = firstvalue(38)
      var fgddm = firstvalue(39)


      for (elem <- f._2) {
        Fbje_sum = Fbje_sum +BigDecimal(elem(5))
        Fsje_sum = Fsje_sum+BigDecimal(elem(6))
        FBsl_sum = FBsl_sum+BigDecimal(elem(7))
        FSsl_sum = FSsl_sum+BigDecimal(elem(8))
        Fbyj_sum = Fbyj_sum+BigDecimal(elem(9))
        Fsyj_sum = Fsyj_sum+BigDecimal(elem(10))
        FBjsf_sum = FBjsf_sum+BigDecimal(elem(11))
        FSjsf_sum = FSjsf_sum+BigDecimal(elem(12))
        Fbyhs_sum = Fbyhs_sum+BigDecimal(elem(13))
        Fsyhs_sum = Fsyhs_sum+BigDecimal(elem(14))
        FBzgf_sum = FBzgf_sum+BigDecimal(elem(15))
        FSzgf_sum = FSzgf_sum+BigDecimal(elem(16))
        FBghf_sum = FBghf_sum+BigDecimal(elem(17))
        FSghf_sum = FSghf_sum+BigDecimal(elem(18))

        FBfxj_sum = FBfxj_sum+BigDecimal(elem(21))
        FSfxj_sum = FSfxj_sum+BigDecimal(elem(22))
        Fbsfje_sum = Fbsfje_sum+BigDecimal(elem(23))
        Fsssje_sum = Fsssje_sum+BigDecimal(elem(24))
      }
     val values = Fdate+","+
                  FinDate+","+
                  FZqdm+","+
                  FSzsh+","+
                  Fjyxwh+","+
                  Fbje_sum.formatted("%.2f")+","+
                  Fsje_sum.formatted("%.2f")+","+
                  FBsl_sum.formatted("%.2f")+","+
                  FSsl_sum.formatted("%.2f")+","+
                  Fbyj_sum.formatted("%.2f")+","+
                  Fsyj_sum.formatted("%.2f")+","+
                  FBjsf_sum.formatted("%.2f")+","+
                  FSjsf_sum.formatted("%.2f")+","+
                  Fbyhs_sum.formatted("%.2f")+","+
                  Fsyhs_sum.formatted("%.2f")+","+
                  FBzgf_sum.formatted("%.2f")+","+
                  FSzgf_sum.formatted("%.2f")+","+
                  FBghf_sum.formatted("%.2f")+","+
                  FSghf_sum.formatted("%.2f")+","+
                  FBgzlx+","+
                  FSgzlx+","+
                  FBfxj_sum.formatted("%.2f")+","+
                  FSfxj_sum.formatted("%.2f")+","+
                  Fbsfje_sum.formatted("%.2f")+","+
                  Fsssje_sum.formatted("%.2f")+","+
                  FZqbz+","+
                  Fywbz+","+
                  FQsbz+","+
                  FBQTF+","+
                  FSQTF+","+
                  ZQDM+","+
                  FJYFS+","+
                  Fsh+","+
                  FZZR+","+
                  FCHK+","+
                  fzlh+","+
                  ftzbz+","+
                  FBQsghf+","+
                  FsQsghf+","+
                  fgddm
      values
    })

    value_final.repartition(1).saveAsTextFile(path.toString)

    val name ="Fdate,FinDate,FZqdm,FSzsh,Fjyxwh,Fbje,Fsje,FBsl,FSsl,Fbyj,Fsyj,FBjsf,FSjsf,Fbyhs,Fsyhs,FBzgf,FSzgf,FBghf,FSghf,FBgzlx,FSgzlx,FBfxj,FSfxj,Fbsfje,Fsssje,FZqbz,Fywbz,FQsbz,FBQTF,FSQTF,ZQDM,FJYFS,Fsh,FZZR,FCHK,fzlh,ftzbz,FBQsghf,FsQsghf,fgddm"
    val field =name.split(",").map(f=>StructField(f,StringType,nullable = true))
    val schema = StructType(field)

    val rowRDD = value_final
      .map(_.split(","))
      .map(f =>  Row(f(0),f(1),f(2),f(3),f(4),f(5),f(6),f(7),f(8),f(9),
                     f(10),f(11),f(12),f(13),f(14),f(15),f(16),f(17),f(18),f(19),
                     f(20),f(21),f(22),f(23),f(24),f(25),f(26),f(27),f(28),f(29),
                     f(30),f(31),f(32),f(33),f(34),f(35),f(36),f(37),f(38),f(39)))

    val execution_dataframe = spark.createDataFrame(rowRDD, schema)
    val url ="jdbc:mysql://192.168.102.119:3306/test?useUnicode=true&characterEncoding=UTF-8"
    val table = "executest"
    val prop = new Properties()
    prop.setProperty("user","hive")
    prop.setProperty("password","hive1234")

    execution_dataframe.write.mode(SaveMode.Append).jdbc(url,table,prop)
    execution_dataframe.repartition(1).write.format("csv")
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .save(output)
  }
}
