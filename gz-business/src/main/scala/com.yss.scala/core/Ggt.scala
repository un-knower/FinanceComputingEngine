package com.yss.scala.core

import java.io.File
import java.net.URI
import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.{Date, Properties}

import com.yss.scala.dto._
import com.yss.scala.util.BasicUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.math.BigDecimal.RoundingMode

/**
  * 港股通
  */
object Ggt {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Ggt")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    //获取配置参数
    val (commonUrl, currentDate,ggtHadoopPath, hadoopRpcPath) = loadInitParam(args)

    val (jsmxFiles,tzxxFiles) = getHadoopFilesName(hadoopRpcPath, ggtHadoopPath, "jsmx", "tzxx")

    //过滤不需要的数据
    val jsmxdbfDF = filterYWLX(spark,jsmxFiles).persist(StorageLevel.MEMORY_ONLY)
    val tzxxDF = getHktzxxDF(spark,tzxxFiles).persist(StorageLevel.MEMORY_ONLY)

    var qsrq = currentDate
    val qsrqformat = new SimpleDateFormat("yyyy-MM-dd").format(new SimpleDateFormat("yyyyMMdd").parse(qsrq))

    //获取原始数据的GDDM数据
    val gddmPairRDD = jsmxdbfDF.select("ZQZH").distinct().rdd.map(item => (trim(item.getAs[String]("ZQZH")), 1)).persist(StorageLevel.MEMORY_ONLY)

    //获取原始数据的XWH数据
    val xwhPairRDD = jsmxdbfDF.select("XWH1").distinct().rdd.map(item => (trim(item.getAs[String]("XWH1")), 1)).persist(StorageLevel.MEMORY_ONLY)

    //获取原始数据的zqdm1数据
    val zqdm1PairRDD = jsmxdbfDF.select("ZQDM1").distinct().rdd.map(item =>(trim(item.getAs("ZQDM1")), 1)).persist(StorageLevel.MEMORY_ONLY)

    //获取原始数据的(XWH,gddm)数据
    val xwhGddmPairRDD = jsmxdbfDF.select("XWH1", "ZQZH").distinct().rdd.map(item => (trim(item.getAs[String]("XWH1")), trim(item.getAs[String]("ZQZH")))).persist(StorageLevel.MEMORY_ONLY)

    //gddm和套账号fsetcode的对应关系
    val gddmFsetCodeRDD = buildGddmFsetCode(gddmPairRDD, spark, commonUrl, currentDate).persist(StorageLevel.MEMORY_ONLY)

    //xwh和fsetcode的关系
    val xwhFsetcodeRDD = xwhGddmPairRDD.map(item => (item._2, item._1)).join(gddmFsetCodeRDD).map(item => (item._2._1, item._2._2)).persist(StorageLevel.MEMORY_ONLY)

    //gddm为key相关参数信息
    val gddmParamRDD = builGddmKeyParamRDD(gddmFsetCodeRDD, spark, commonUrl, currentDate).persist(StorageLevel.MEMORY_ONLY)

    //构建gddm为key的参数信息的map结构 [gddm, params], 并broadcast
    val broadcastGddmParamMap = spark.sparkContext.broadcast(gddmParamRDD.collectAsMap())

    //根据原始字段zqdm1计算证券代码
    val zqdm1ValueRDD = buildZqdm1ValueRDD(zqdm1PairRDD, tzxxDF,spark, qsrq).persist(StorageLevel.MEMORY_ONLY)
    val zqdm1ValueMap = buildZqdm1ValueMap(zqdm1ValueRDD)
    val broadcastZqdm1ValueMap = spark.sparkContext.broadcast(zqdm1ValueMap)

    //计算gddm和FJJLx 和 FJJLB的关系， 判断ZS股票条件1
    val gddmFJJLxLBMap = buildGddmFJJLxLBRDD(gddmFsetCodeRDD, spark, commonUrl, currentDate, qsrq)
    val broadcastGddmFJJLxLBMap = spark.sparkContext.broadcast(gddmFJJLxLBMap)

    //判断ZS股票的条件2
    val gddmGsdmZslMap = buildExistsZSbyGsdm(xwhFsetcodeRDD, spark, commonUrl, currentDate, qsrqformat)
    val broadcastGddmGsdmZslMap = spark.sparkContext.broadcast(gddmGsdmZslMap)

    //判断ZS股票的条件3
    val zqdm1ZS2Map = buildExistsZSbyZqdm(zqdm1ValueRDD, spark, commonUrl, currentDate, qsrqformat)
    val brocastZqdm1ZS2Map = spark.sparkContext.broadcast(zqdm1ZS2Map)

    //转换佣金利率Map
    val yjlvMap = buildYjlbMap(spark, commonUrl, currentDate)
    val broadcastYjlvMap = spark.sparkContext.broadcast(yjlvMap)

    //计算各个类型的费用
    val ffyffsMap = buildFFYFS(xwhGddmPairRDD, spark, commonUrl, currentDate, qsrq)
    val broadcastFFyffsMap = spark.sparkContext.broadcast(ffyffsMap)

    //抽取计算的数据
    import spark.implicits._
    val calculationDF = buildCalculationDF(jsmxdbfDF, spark, gddmParamRDD)
    val basicsRDD = calculationDF.as[HkJsmxModel].rdd.persist(StorageLevel.MEMORY_ONLY)

    /*-----------------计算费用开始---------------------*/
    //blnFyCjBhMx==0&&blnFySqBhMx==0
    val basics00RDD = basicsRDD.filter { item =>
      val blnFyCjBhMx = getValueFromStr(item.paraminfos, "BLNFYCJBHMX")
      val blnFySqBhMx = getValueFromStr(item.paraminfos, "BLNFYSQBHMX")
      if (blnFyCjBhMx.equals("0") && blnFySqBhMx.equals("0")) true else false
    }

    val cjBhMxSqBhMx00DF = buildCjBhMxSqBhMx00(basics00RDD, spark,
      broadcastGddmParamMap, broadcastZqdm1ValueMap,
      broadcastGddmFJJLxLBMap, broadcastGddmGsdmZslMap,
      brocastZqdm1ZS2Map, broadcastYjlvMap, broadcastFFyffsMap)

    //blnFyCjBhMx==1时
    val basicsl0RDD = basicsRDD.filter { item =>
      val blnFyCjBhMx = getValueFromStr(item.paraminfos, "BLNFYCJBHMX")
      if (blnFyCjBhMx.equals("1")) true else false
    }

    val cjBhMx1DF = buildCjBhMx1(basicsl0RDD, spark,
      broadcastGddmParamMap, broadcastZqdm1ValueMap,
      broadcastGddmFJJLxLBMap, broadcastGddmGsdmZslMap,
      brocastZqdm1ZS2Map, broadcastYjlvMap, broadcastFFyffsMap)

    //blnFySqBhMx==1时
    val basics01RDD = basicsRDD.filter { item =>
      val blnFySqBhMx = getValueFromStr(item.paraminfos, "BLNFYSQBHMX")
      if (blnFySqBhMx.equals("1")) true else false
    }

    val sqBhMx1DF = buildSqBhMx1(basics01RDD, spark,
      broadcastGddmParamMap, broadcastZqdm1ValueMap,
      broadcastGddmFJJLxLBMap, broadcastGddmGsdmZslMap,
      brocastZqdm1ZS2Map, broadcastYjlvMap, broadcastFFyffsMap)

    //blnFyCjBhMx==1&&blnFySqBhMx==1
    val basicsl1RDD = basicsRDD.filter { item =>
      val blnFyCjBhMx = getValueFromStr(item.paraminfos, "BLNFYCJBHMX")
      val blnFySqBhMx = getValueFromStr(item.paraminfos, "BLNFYSQBHMX")
      if (blnFyCjBhMx.equals("1") && blnFySqBhMx.equals("1")) true else false
    }

    val cjBhMxSqBhMx11DF = buildCjBhMxSqBhMx11(basicsl1RDD, spark,
      broadcastGddmParamMap, broadcastZqdm1ValueMap,
      broadcastGddmFJJLxLBMap, broadcastGddmGsdmZslMap,
      brocastZqdm1ZS2Map, broadcastYjlvMap, broadcastFFyffsMap)
    /*-----------------计算费用结束---------------------*/


    /*-----------------计算佣金开始---------------------*/
    //blnYjCjBhMx==0 & blnYjSqBhMx==0
    val blnYj00RDD = basicsRDD.filter{ item =>
      val blnYjCjBhMx = getValueFromStr(item.paraminfos, "BLNYJCJBHMX")
      val blnYjSqBhMx = getValueFromStr(item.paraminfos, "BLNYJSQBHMX")
      if (blnYjCjBhMx.equals("0") && blnYjSqBhMx.equals("0")) true else false
    }

    val blnYj00DF = buildblnYj00RDD(blnYj00RDD, spark,
      broadcastGddmParamMap, broadcastZqdm1ValueMap,
      broadcastGddmFJJLxLBMap, broadcastGddmGsdmZslMap,
      brocastZqdm1ZS2Map, broadcastYjlvMap, broadcastFFyffsMap)

    //blnYjCjBhMx=1
    val blnYjCjBhMx1RDD = basicsRDD.filter{ item =>
      val blnYjCjBhMx = getValueFromStr(item.paraminfos, "BLNYJCJBHMX")
      if (blnYjCjBhMx.equals("1")) true else false
    }

    val blnYjCjBhMx1DF = buildBlnYjCjBhMx1(blnYjCjBhMx1RDD, spark,
      broadcastGddmParamMap, broadcastZqdm1ValueMap,
      broadcastGddmFJJLxLBMap, broadcastGddmGsdmZslMap,
      brocastZqdm1ZS2Map, broadcastYjlvMap, broadcastFFyffsMap)

    //blnYjSqBhMx=1
    val blnYjSqBhMx1RDD = basicsRDD.filter{ item =>
      val blnYjSqBhMx = getValueFromStr(item.paraminfos, "BLNYJSQBHMX")
      if (blnYjSqBhMx.equals("1")) true else false
    }

    val blnYjSqBhMx1DF = buildBlnYjSqBhMx1(blnYjSqBhMx1RDD, spark,
      broadcastGddmParamMap, broadcastZqdm1ValueMap,
      broadcastGddmFJJLxLBMap, broadcastGddmGsdmZslMap,
      brocastZqdm1ZS2Map, broadcastYjlvMap, broadcastFFyffsMap)
    /*-----------------计算费用结束---------------------*/

    //费用汇总DF
    val fyResultDF = cjBhMxSqBhMx00DF.union(cjBhMx1DF).union(sqBhMx1DF).union(cjBhMxSqBhMx11DF)
    //佣金汇总DF
    val yjResultDF = blnYj00DF.union(blnYjCjBhMx1DF).union(blnYjSqBhMx1DF)

    fyResultDF.createOrReplaceTempView("fyResultDF_table")
    yjResultDF.createOrReplaceTempView("yjResultDF_table")

    val resutlDF = spark.sql(
        " SELECT t1.FSETID, t1.FDATE, t1.FINDATE, t1.FZQDM, t1.FJYXWH, t1.FZQBZ, t1.FYWBZ, t1.ZQDM, t1.FBS, " +
        " t1.FQTF, t1.FJE, t1.FSL, t2.fyj FYJ, t1.FJSF, t1.FYHS," +
        " t1.FZGF, t1.FGHF, t1.FGZLX, t1.FHGGAIN, t1.FFXJ, t2.Fsssje FSFJE," +
        " t1.FSZSH, t1.FQSBZ, t1.FJYFS," +
        " t1.FSH, t1.FZZR, t1.FCHK, t1.FZLH, t1.FTZBZ, t1.FQSGHF," +
        " t1.FGDDM, t1.FJYBZ, t1.ISRTGS, t1.FPARTID, t1.FHTXH," +
        " t1.FCSHTXH, t1.FRZLV, t1.FCSGHQX, t1.FSJLY, t1.FBZ," +
        " t1.FBY1, t1.FBY2, t1.FBY3, t1.FBY4, t1.FBY5" +
        " from " +
        " fyResultDF_table t1 join yjResultDF_table t2 on " +
        " t1.FDATE = t2.FDATE and t1.FINDATE = t2.FINDATE and t1.FZQDM =t2.FZQDM " +
        " and t1.FJYXWH = t2.FJYXWH and t1.FZQBZ = t2.FZQBZ and t1.ZQDM = t2.ZQDM and t1.FBS = t2.FBS"
    )


    val format :DecimalFormat  = new DecimalFormat("0.00")
    val resultDS = resutlDF.as[Hzjkqs].rdd.map{ item =>
          Hzjkqs(
            item.FSETID,
            item.FDATE,
            item.FINDATE,
            item.FZQDM,
            item.FSZSH,
            item.FJYXWH,
            item.FBS,
            format.format(item.FJE.toDouble),
            format.format(item.FSL.toDouble),
            format.format(item.FYJ.toDouble),
            format.format(item.FJSF.toDouble),
            format.format(item.FYHS.toDouble),
            format.format(item.FZGF.toDouble),
            format.format(item.FGHF.toDouble),
            format.format(item.FFXJ.toDouble),
            format.format(item.FQTF.toDouble),
            format.format(item.FGZLX.toDouble),
            format.format(item.FHGGAIN.toDouble),
            format.format(item.FSFJE.toDouble),
            item.FZQBZ,
            item.FYWBZ,
            item.FJYBZ,
            item.FQSBZ,
            item.ZQDM,
            item.FJYFS,
            item.FSH,
            item.FZZR,
            item.FCHK,
            item.FZLH,
            item.FTZBZ,
            item.FQSGHF,
            item.FGDDM,
            item.ISRTGS,
            item.FPARTID,
            item.FHTXH ,
            item.FCSHTXH,
            item.FRZLV,
            item.FCSGHQX,
            item.FSJLY,
            item.FBZ,
            item.FBY1,
            item.FBY2,
            item.FBY3,
            item.FBY4,
            item.FBY5)
        }.toDS()

    val resultOrder = resultDS.orderBy("FDATE", "FINDATE", "FZQDM", "FJYXWH", "FZQBZ", "FYWBZ","ZQDM")

    //输出mysql
    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "root1234")
    properties.setProperty("driver", "com.mysql.jdbc.Driver")
    resultOrder.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://192.168.102.120/JJCWGZ?useUnicode=true&characterEncoding=utf8", "ggt_table", properties)
  }

  /**
    * //blnFyCjBhMx==1&&blnFySqBhMx==1
    * @param jsmxModelRDD
    */
  def buildCjBhMxSqBhMx11(jsmxModelRDD:RDD[HkJsmxModel], spark:SparkSession,
                          bdGddmParamMap:Broadcast[scala.collection.Map[String,String]],
                          bdZqdm1ValueMap:Broadcast[scala.collection.Map[String,String]],
                          bdGddmFJJLxLBMap:Broadcast[scala.collection.Map[String,String]],
                          bdGddmGsdmZslMap:Broadcast[scala.collection.Map[String,String]],
                          bdZqdm1ZS2Map:Broadcast[scala.collection.Map[String,String]],
                          bdYjlvMap:Broadcast[scala.collection.Map[String,String]],
                          bdFFyffsMap:Broadcast[scala.collection.Map[String,String]]) ={
    import spark.implicits._
    jsmxModelRDD.toDS().createOrReplaceTempView("jsmxModels11_table")
    val cjBhMxSqBhMx11DF = spark.sql(
      " select ywlx,bdlx,qsrq,jsrq,xwh1, zqzh, zqdm1,qsbz, mmbz,wbhl,cjbh,sqbh, "+
        " sum(sl) sl, sum(cjsl) cjsl, sum(wbje) wbje, sum(yhs) yhs, " +
        " sum(jyzf) jyzf,sum(jyf) jyf, sum(syf) syf, sum(jsf) jsf, " +
        " sum(qtje) qtje, sum(wbysf) wbysf,sum(ysfje) ysfje,min(paraminfos) paraminfos"+
        " from jsmxModels11_table "+
        " group by ywlx,bdlx,qsrq,jsrq,xwh1, zqzh, zqdm1,qsbz, mmbz,wbhl,cjbh,sqbh "
    )

    val cjBhMxSqBhMx11Result = cjBhMxSqBhMx11DF.rdd.map {
      calculationResult(_,bdGddmParamMap,bdZqdm1ValueMap,bdGddmFJJLxLBMap,bdGddmGsdmZslMap,bdZqdm1ZS2Map,bdYjlvMap,bdFFyffsMap)}


    cjBhMxSqBhMx11Result.toDS().createOrReplaceTempView("CJSQ11_TALBE")

    spark.sql(
      " SELECT FDATE,FINDATE,FZQDM,FJYXWH,FZQBZ,FYWBZ,ZQDM, FBS," +
        " SUM(FQTF) FQTF, SUM(FJE) FJE, SUM(FSL) FSL, SUM(FYJ) FYJ, SUM(FJSF) FJSF, SUM(FYHS) FYHS," +
        " SUM(FZGF) FZGF, SUM(FGHF) FGHF, SUM(FGZLX) FGZLX, SUM(FHGGAIN) FHGGAIN, SUM(FFXJ) FFXJ, SUM(FSFJE) FSFJE," +
        " MIN(FSZSH) FSZSH, MIN(FQSBZ) FQSBZ, MIN(FJYFS) FJYFS," +
        " MIN(FSH) FSH, MIN(FZZR) FZZR, MIN(FCHK) FCHK, MIN(FZLH) FZLH, MIN(FTZBZ) FTZBZ, MIN(FQSGHF) FQSGHF," +
        " MIN(FGDDM) FGDDM, MIN(FJYBZ) FJYBZ, MIN(ISRTGS) ISRTGS, MIN(FPARTID) FPARTID, MIN(FHTXH) FHTXH," +
        " MIN(FCSHTXH) FCSHTXH, MIN(FRZLV) FRZLV, MIN(FCSGHQX) FCSGHQX, MIN(FSJLY) FSJLY, " +
        " MIN(FSETID) FSETID, MIN(FBZ) FBZ, MIN(FBY1) FBY1,MIN(FBY2) FBY2,MIN(FBY3) FBY3,MIN(FBY4) FBY4,MIN(FBY5) FBY5" +
        " FROM CJSQ11_TALBE " +
        " GROUP BY FDATE,FINDATE,FZQDM,FJYXWH,FZQBZ,FYWBZ,ZQDM,FBS")
  }

  /**
    * //blnFySqBhMx==1时
    * @param jsmxModelRDD
    */
  def buildSqBhMx1(jsmxModelRDD:RDD[HkJsmxModel], spark:SparkSession,
                   bdGddmParamMap:Broadcast[scala.collection.Map[String,String]],
                   bdZqdm1ValueMap:Broadcast[scala.collection.Map[String,String]],
                   bdGddmFJJLxLBMap:Broadcast[scala.collection.Map[String,String]],
                   bdGddmGsdmZslMap:Broadcast[scala.collection.Map[String,String]],
                   bdZqdm1ZS2Map:Broadcast[scala.collection.Map[String,String]],
                   bdYjlvMap:Broadcast[scala.collection.Map[String,String]],
                   bdFFyffsMap:Broadcast[scala.collection.Map[String,String]]) = {
    import spark.implicits._

    val sqBhMx1DF = conditionGroupBy3(jsmxModelRDD, spark)

    val sqBhMx1Result = sqBhMx1DF.rdd.map {
      calculationResult(_,bdGddmParamMap,bdZqdm1ValueMap,bdGddmFJJLxLBMap,bdGddmGsdmZslMap,bdZqdm1ZS2Map,bdYjlvMap,bdFFyffsMap)}

    sqBhMx1Result.toDS().createOrReplaceTempView("SQBH1_TALBE")

    spark.sql(
        " SELECT FDATE,FINDATE,FZQDM,FJYXWH,FZQBZ,FYWBZ,ZQDM, FBS," +
        " SUM(FQTF) FQTF, SUM(FJE) FJE, SUM(FSL) FSL, SUM(FYJ) FYJ, SUM(FJSF) FJSF, SUM(FYHS) FYHS," +
        " SUM(FZGF) FZGF, SUM(FGHF) FGHF, SUM(FGZLX) FGZLX, SUM(FHGGAIN) FHGGAIN, SUM(FFXJ) FFXJ, SUM(FSFJE) FSFJE," +
        " MIN(FSZSH) FSZSH, MIN(FQSBZ) FQSBZ, MIN(FJYFS) FJYFS," +
        " MIN(FSH) FSH, MIN(FZZR) FZZR, MIN(FCHK) FCHK, MIN(FZLH) FZLH, MIN(FTZBZ) FTZBZ, MIN(FQSGHF) FQSGHF," +
        " MIN(FGDDM) FGDDM, MIN(FJYBZ) FJYBZ, MIN(ISRTGS) ISRTGS, MIN(FPARTID) FPARTID, MIN(FHTXH) FHTXH," +
        " MIN(FCSHTXH) FCSHTXH, MIN(FRZLV) FRZLV, MIN(FCSGHQX) FCSGHQX, MIN(FSJLY) FSJLY, " +
        " MIN(FSETID) FSETID, MIN(FBZ) FBZ, MIN(FBY1) FBY1,MIN(FBY2) FBY2,MIN(FBY3) FBY3,MIN(FBY4) FBY4,MIN(FBY5) FBY5" +
        " FROM SQBH1_TALBE " +
        " GROUP BY FDATE,FINDATE,FZQDM,FJYXWH,FZQBZ,FYWBZ,ZQDM,FBS")
  }

  /**
    * //blnFyCjBhMx==1时
    * @param jsmxModelRDD
    */
  def buildCjBhMx1(jsmxModelRDD:RDD[HkJsmxModel], spark:SparkSession,
                   bdGddmParamMap:Broadcast[scala.collection.Map[String,String]],
                   bdZqdm1ValueMap:Broadcast[scala.collection.Map[String,String]],
                   bdGddmFJJLxLBMap:Broadcast[scala.collection.Map[String,String]],
                   bdGddmGsdmZslMap:Broadcast[scala.collection.Map[String,String]],
                   bdZqdm1ZS2Map:Broadcast[scala.collection.Map[String,String]],
                   bdYjlvMap:Broadcast[scala.collection.Map[String,String]],
                   bdFFyffsMap:Broadcast[scala.collection.Map[String,String]]) = {

    import spark.implicits._
    val cjBhMx1DF = conditionGroupBy2(jsmxModelRDD, spark)

    val cjBhMx1Result = cjBhMx1DF.rdd.map {
      calculationResult(_,bdGddmParamMap,bdZqdm1ValueMap,bdGddmFJJLxLBMap,bdGddmGsdmZslMap,bdZqdm1ZS2Map,bdYjlvMap,bdFFyffsMap)}
    cjBhMx1Result.toDS().createOrReplaceTempView("CJBH1_TALBE")

    spark.sql(
        " SELECT FDATE,FINDATE,FZQDM,FJYXWH,FZQBZ,FYWBZ,ZQDM,FBS," +
        " SUM(FQTF) FQTF, SUM(FJE) FJE, SUM(FSL) FSL, SUM(FYJ) FYJ, SUM(FJSF) FJSF, SUM(FYHS) FYHS," +
        " SUM(FZGF) FZGF, SUM(FGHF) FGHF, SUM(FGZLX) FGZLX, SUM(FHGGAIN) FHGGAIN, SUM(FFXJ) FFXJ, SUM(FSFJE) FSFJE," +
        " MIN(FSZSH) FSZSH, MIN(FQSBZ) FQSBZ, MIN(FJYFS) FJYFS," +
        " MIN(FSH) FSH, MIN(FZZR) FZZR, MIN(FCHK) FCHK, MIN(FZLH) FZLH, MIN(FTZBZ) FTZBZ, MIN(FQSGHF) FQSGHF," +
        " MIN(FGDDM) FGDDM, MIN(FJYBZ) FJYBZ, MIN(ISRTGS) ISRTGS, MIN(FPARTID) FPARTID, MIN(FHTXH) FHTXH," +
        " MIN(FCSHTXH) FCSHTXH, MIN(FRZLV) FRZLV, MIN(FCSGHQX) FCSGHQX, MIN(FSJLY) FSJLY, " +
        " MIN(FSETID) FSETID, MIN(FBZ) FBZ, MIN(FBY1) FBY1,MIN(FBY2) FBY2,MIN(FBY3) FBY3,MIN(FBY4) FBY4,MIN(FBY5) FBY5 " +
        " FROM CJBH1_TALBE " +
        " GROUP BY FDATE,FINDATE,FZQDM,FJYXWH,FZQBZ,FYWBZ,ZQDM,FBS")
  }

  /**
    * sum 根据YWLX,bdlx,qsrq,JSRQ,xwh1, zqzh, zqdm1,qsbz, mmbz,wbhl 汇总
    * @param basicsRDD
    * @param spark
    * @return
    */
  def conditionGroupBy1(basicsRDD:RDD[HkJsmxModel], spark:SparkSession) = {
    import spark.implicits._
    basicsRDD.toDS().createOrReplaceTempView("jsmxModels00_table")
    spark.sql(
    " select ywlx,bdlx,qsrq,jsrq,xwh1, zqzh, zqdm1,qsbz, mmbz,wbhl, "+
    " sum(sl) sl, sum(cjsl) cjsl, sum(wbje) wbje, sum(yhs) yhs, " +
    " sum(jyzf) jyzf,sum(jyf) jyf, sum(syf) syf, sum(jsf) jsf, " +
    " sum(qtje) qtje, sum(wbysf) wbysf,sum(ysfje) ysfje,min(paraminfos) paraminfos"+
    " from jsmxModels00_table "+
    " group by ywlx,bdlx,qsrq,jsrq,xwh1, zqzh, zqdm1,qsbz, mmbz,wbhl "
    )
  }

  /**
    * sum 根据YWLX,bdlx,qsrq,JSRQ,xwh1, zqzh, zqdm1,qsbz, mmbz,wbhl,CJBH 汇总
    * @param basicsRDD
    * @param spark
    * @return
    */
  def conditionGroupBy2(basicsRDD:RDD[HkJsmxModel], spark:SparkSession) = {
    import spark.implicits._
    basicsRDD.toDS().createOrReplaceTempView("jsmxModels10_table")
    spark.sql(
      " select ywlx,bdlx,qsrq,jsrq,xwh1, zqzh, zqdm1,qsbz, mmbz,wbhl,cjbh, "+
        " sum(sl) sl, sum(cjsl) cjsl, sum(wbje) wbje, sum(yhs) yhs, " +
        " sum(jyzf) jyzf,sum(jyf) jyf, sum(syf) syf, sum(jsf) jsf, " +
        " sum(qtje) qtje, sum(wbysf) wbysf,sum(ysfje) ysfje,min(paraminfos) paraminfos"+
        " from jsmxModels10_table "+
        " group by ywlx,bdlx,qsrq,jsrq,xwh1, zqzh, zqdm1,qsbz, mmbz,wbhl,cjbh "
    )
  }

  /**
    * sum 根据YWLX,bdlx,qsrq,JSRQ,xwh1, zqzh, zqdm1,qsbz, mmbz,wbhl,SQBH 汇总
    * @param basicsRDD
    * @param spark
    * @return
    */
  def conditionGroupBy3(basicsRDD:RDD[HkJsmxModel], spark:SparkSession)={
    import spark.implicits._
    basicsRDD.toDS().createOrReplaceTempView("jsmxModels01_table")

    spark.sql(
      " select ywlx,bdlx,qsrq,jsrq,xwh1, zqzh, zqdm1,qsbz, mmbz,wbhl,sqbh, "+
        " sum(sl) sl, sum(cjsl) cjsl, sum(wbje) wbje, sum(yhs) yhs, " +
        " sum(jyzf) jyzf,sum(jyf) jyf, sum(syf) syf, sum(jsf) jsf, " +
        " sum(qtje) qtje, sum(wbysf) wbysf,sum(ysfje) ysfje,min(paraminfos) paraminfos"+
        " from jsmxModels01_table "+
        " group by ywlx,bdlx,qsrq,jsrq,xwh1, zqzh, zqdm1,qsbz, mmbz,wbhl,sqbh "
    )
  }

  /**
    * blnFyCjBhMx==0&&blnFySqBhMx==0
    * @param basicsRDD
    * @param spark
    * @param bdGddmParamMap       相关参数Map[gddm, 相关参数]
    * @param bdZqdm1ValueMap      原始数据Zqdm1字段，判断证券代码 [zqdm1, 证券代码的值]
    * @param bdGddmFJJLxLBMap     (gddm, fjjlx_fjjlb) 判断业务标识
    * @param bdGddmGsdmZslMap     (xwh, zsl)
    * @param bdZqdm1ZS2Map        (zqdm1, zs2)
    * @param bdYjlvMap            (套帐号_证券类别_证券市场_席位代码(股东代码), 费用费率_佣金最低值_折扣率)
    *                             (fsetCode+"_"+fzqlb+"_"+fszsh+"_"+fStr1, fLv+"_"+fLvMin+"_"+flvzk)
    * @param bdFFyffsMap          (xwh+"_"+gddm+"_"+fzqlb 费用1=0_费用2=1)
    * @return
    */
  def buildCjBhMxSqBhMx00(basicsRDD:RDD[HkJsmxModel], spark:SparkSession,
                          bdGddmParamMap:Broadcast[scala.collection.Map[String,String]],
                          bdZqdm1ValueMap:Broadcast[scala.collection.Map[String,String]],
                          bdGddmFJJLxLBMap:Broadcast[scala.collection.Map[String,String]],
                          bdGddmGsdmZslMap:Broadcast[scala.collection.Map[String,String]],
                          bdZqdm1ZS2Map:Broadcast[scala.collection.Map[String,String]],
                          bdYjlvMap:Broadcast[scala.collection.Map[String,String]],
                          bdFFyffsMap:Broadcast[scala.collection.Map[String,String]]) = {
    import spark.implicits._
    val cjBhMxSqBhMx00DF = conditionGroupBy1(basicsRDD, spark)
    val cjBhMxSqBhMx00Result = cjBhMxSqBhMx00DF.rdd.map { item=>
      calculationResult(item,bdGddmParamMap,bdZqdm1ValueMap,bdGddmFJJLxLBMap,bdGddmGsdmZslMap,bdZqdm1ZS2Map,bdYjlvMap,bdFFyffsMap)
    }

    cjBhMxSqBhMx00Result.toDS().createOrReplaceTempView("CJSQ_TALBE")

    spark.sql(
        " SELECT FDATE,FINDATE,FZQDM,FJYXWH,FZQBZ,FYWBZ,ZQDM, FBS," +
        " SUM(FQTF) FQTF, SUM(FJE) FJE, SUM(FSL) FSL, SUM(FYJ) FYJ, SUM(FJSF) FJSF, SUM(FYHS) FYHS," +
        " SUM(FZGF) FZGF, SUM(FGHF) FGHF, SUM(FGZLX) FGZLX, SUM(FHGGAIN) FHGGAIN, SUM(FFXJ) FFXJ, SUM(FSFJE) FSFJE," +
        " MIN(FSZSH) FSZSH, MIN(FQSBZ) FQSBZ, MIN(FJYFS) FJYFS," +
        " MIN(FSH) FSH, MIN(FZZR) FZZR, MIN(FCHK) FCHK, MIN(FZLH) FZLH, MIN(FTZBZ) FTZBZ, MIN(FQSGHF) FQSGHF," +
        " MIN(FGDDM) FGDDM, MIN(FJYBZ) FJYBZ, MIN(ISRTGS) ISRTGS, MIN(FPARTID) FPARTID, MIN(FHTXH) FHTXH," +
        " MIN(FCSHTXH) FCSHTXH, MIN(FRZLV) FRZLV, MIN(FCSGHQX) FCSGHQX, MIN(FSJLY) FSJLY, " +
        " MIN(FSETID) FSETID, MIN(FBZ) FBZ, MIN(FBY1) FBY1,MIN(FBY2) FBY2,MIN(FBY3) FBY3,MIN(FBY4) FBY4,MIN(FBY5) FBY5" +
        " FROM CJSQ_TALBE " +
        " GROUP BY FDATE,FINDATE,FZQDM,FJYXWH,FZQBZ,FYWBZ,ZQDM,FBS")
  }

  /**
    * 计算佣金"blnFyCjBhMx==0&&blnFySqBhMx==0"

    * @param basicsRDD
    * @param spark
    * @param bdGddmParamMap
    * @param bdZqdm1ValueMap
    * @param bdGddmFJJLxLBMap
    * @param bdGddmGsdmZslMap
    * @param bdZqdm1ZS2Map
    * @param bdYjlvMap
    * @param bdFFyffsMap
    * @return
    */
  def buildblnYj00RDD(basicsRDD:RDD[HkJsmxModel], spark:SparkSession,
                          bdGddmParamMap:Broadcast[scala.collection.Map[String,String]],
                          bdZqdm1ValueMap:Broadcast[scala.collection.Map[String,String]],
                          bdGddmFJJLxLBMap:Broadcast[scala.collection.Map[String,String]],
                          bdGddmGsdmZslMap:Broadcast[scala.collection.Map[String,String]],
                          bdZqdm1ZS2Map:Broadcast[scala.collection.Map[String,String]],
                          bdYjlvMap:Broadcast[scala.collection.Map[String,String]],
                          bdFFyffsMap:Broadcast[scala.collection.Map[String,String]]) : DataFrame = {
    import spark.implicits._
    val blnYj00DF = conditionGroupBy1(basicsRDD, spark)

    val blnYj00Result = blnYj00DF.rdd.map { item=>
      yJcalculationResult(item,bdGddmParamMap,bdZqdm1ValueMap,bdGddmFJJLxLBMap,bdGddmGsdmZslMap,bdZqdm1ZS2Map,bdYjlvMap,bdFFyffsMap)
    }

    blnYj00Result.toDS().createOrReplaceTempView("blnYj_table1")

    spark.sql(
      " SELECT FDATE,FINDATE,FZQDM,FJYXWH,FZQBZ,FYWBZ,ZQDM, FBS," +
        " SUM(fyj) fyj, SUM(Fsssje) Fsssje " +
        " FROM blnYj_table1 " +
        " GROUP BY FDATE,FINDATE,FZQDM,FJYXWH,FZQBZ,FYWBZ,ZQDM,FBS")

  }

  /**
    * 计算佣金"blnFyCjBhMx==1
    * @param basicsRDD
    * @param spark
    * @param bdGddmParamMap
    * @param bdZqdm1ValueMap
    * @param bdGddmFJJLxLBMap
    * @param bdGddmGsdmZslMap
    * @param bdZqdm1ZS2Map
    * @param bdYjlvMap
    * @param bdFFyffsMap
    * @return
    */
  def buildBlnYjCjBhMx1(basicsRDD:RDD[HkJsmxModel], spark:SparkSession,
                      bdGddmParamMap:Broadcast[scala.collection.Map[String,String]],
                      bdZqdm1ValueMap:Broadcast[scala.collection.Map[String,String]],
                      bdGddmFJJLxLBMap:Broadcast[scala.collection.Map[String,String]],
                      bdGddmGsdmZslMap:Broadcast[scala.collection.Map[String,String]],
                      bdZqdm1ZS2Map:Broadcast[scala.collection.Map[String,String]],
                      bdYjlvMap:Broadcast[scala.collection.Map[String,String]],
                      bdFFyffsMap:Broadcast[scala.collection.Map[String,String]]) : DataFrame = {
    import spark.implicits._
    val blnYj00DF = conditionGroupBy2(basicsRDD, spark)

    val blnYj00Result = blnYj00DF.rdd.map { item=>
      yJcalculationResult(item,bdGddmParamMap,bdZqdm1ValueMap,bdGddmFJJLxLBMap,bdGddmGsdmZslMap,bdZqdm1ZS2Map,bdYjlvMap,bdFFyffsMap)
    }

    blnYj00Result.toDS().createOrReplaceTempView("blnYj_table2")

    spark.sql(
      " SELECT FDATE,FINDATE,FZQDM,FJYXWH,FZQBZ,FYWBZ,ZQDM, FBS," +
        " SUM(fyj) fyj, SUM(Fsssje) Fsssje " +
        " FROM blnYj_table2 " +
        " GROUP BY FDATE,FINDATE,FZQDM,FJYXWH,FZQBZ,FYWBZ,ZQDM,FBS")

  }

  /**
    * 计算佣金"blnYjSqBhMx==1
    * @param basicsRDD
    * @param spark
    * @param bdGddmParamMap
    * @param bdZqdm1ValueMap
    * @param bdGddmFJJLxLBMap
    * @param bdGddmGsdmZslMap
    * @param bdZqdm1ZS2Map
    * @param bdYjlvMap
    * @param bdFFyffsMap
    * @return
    */
  def buildBlnYjSqBhMx1(basicsRDD:RDD[HkJsmxModel], spark:SparkSession,
                        bdGddmParamMap:Broadcast[scala.collection.Map[String,String]],
                        bdZqdm1ValueMap:Broadcast[scala.collection.Map[String,String]],
                        bdGddmFJJLxLBMap:Broadcast[scala.collection.Map[String,String]],
                        bdGddmGsdmZslMap:Broadcast[scala.collection.Map[String,String]],
                        bdZqdm1ZS2Map:Broadcast[scala.collection.Map[String,String]],
                        bdYjlvMap:Broadcast[scala.collection.Map[String,String]],
                        bdFFyffsMap:Broadcast[scala.collection.Map[String,String]]) : DataFrame = {
    import spark.implicits._
    val blnYj00DF = conditionGroupBy3(basicsRDD, spark)

    val blnYj00Result = blnYj00DF.rdd.map { item=>
      yJcalculationResult(item,bdGddmParamMap,bdZqdm1ValueMap,bdGddmFJJLxLBMap,bdGddmGsdmZslMap,bdZqdm1ZS2Map,bdYjlvMap,bdFFyffsMap)
    }

    blnYj00Result.toDS().createOrReplaceTempView("blnYj_table3")

    spark.sql(
      " SELECT FDATE,FINDATE,FZQDM,FJYXWH,FZQBZ,FYWBZ,ZQDM, FBS," +
        " SUM(fyj) fyj, SUM(Fsssje) Fsssje " +
        " FROM blnYj_table3 " +
        " GROUP BY FDATE,FINDATE,FZQDM,FJYXWH,FZQBZ,FYWBZ,ZQDM,FBS")

  }

  /**
    * 费用计算结果数据
    * @param item
    * @param bdGddmParamMap       相关参数Map[gddm, 相关参数]
    * @param bdZqdm1ValueMap      原始数据Zqdm1字段，判断证券代码 [zqdm1, 证券代码的值]
    * @param bdGddmFJJLxLBMap     (gddm, fjjlx_fjjlb) 判断业务标识
    * @param bdxwhZslMap          (xwh, zsl)
    * @param bdZqdm1ZS2Map        (zqdm1, zs2)
    * @param bdYjlvMap            (套帐号_证券类别_证券市场_席位代码(股东代码), 费用费率_佣金最低值_折扣率)
    *                             (fsetCode+"_"+fzqlb+"_"+fszsh+"_"+fStr1, fLv+"_"+fLvMin+"_"+flvzk)
    * @param bdFFyffsMap          (xwh+"_"+gddm+"_"+fzqlb 费用1=0_费用2=1)
    * @return
    */
  def calculationResult(item: Row,
                        bdGddmParamMap:Broadcast[scala.collection.Map[String,String]],
                        bdZqdm1ValueMap:Broadcast[scala.collection.Map[String,String]],
                        bdGddmFJJLxLBMap:Broadcast[scala.collection.Map[String,String]],
                        bdxwhZslMap:Broadcast[scala.collection.Map[String,String]],
                        bdZqdm1ZS2Map:Broadcast[scala.collection.Map[String,String]],
                        bdYjlvMap:Broadcast[scala.collection.Map[String,String]],
                        bdFFyffsMap:Broadcast[scala.collection.Map[String,String]]):Hzjkqs = {

    val gddmParamMap = bdGddmParamMap.value
    val zqdm1Map = bdZqdm1ValueMap.value
    val gddmfjjlxlbMap = bdGddmFJJLxLBMap.value
    val xwhzslMap = bdxwhZslMap.value
    val zqdm1zs2Map = bdZqdm1ZS2Map.value
    val yjlvMap = bdYjlvMap.value
    val ffyffsMap = bdFFyffsMap.value

    val ywlx:String = item.getAs[String]("ywlx")
    //资产代码
    val FSETID:String=""
    //日期
    val Fdate:String = getFdate(item)
    //日期
    val FinDate:String = item.getAs[String]("qsrq")
    //证券代码
    val FZqdm:String = getFzqdm(item, zqdm1Map)
    //深圳上海
    val FSzsh:String= "F"
    //交易席位号
    val Fjyxwh:String = getFjyxwh(item)
    //证券标志
    val FZqbz:String = getFZqbz(item)
    //业务标志
    val Fywbz:String = getFywbz(item, gddmfjjlxlbMap,xwhzslMap,zqdm1zs2Map)
    //买卖方向
    val fbs:String = buildFbs(item, ywlx)
    //数量
    val Fsl:BigDecimal = buildFsl(item, ywlx)
    //经手费
    val Fjsf:BigDecimal = buildFjsf(item, ywlx)
    //印花税
    val Fyhs:BigDecimal = buildFyhs(item, ywlx)
    //征管费
    val Fzgf:BigDecimal = buildFzgf(item, ywlx)
    //过户费
    val Fghf:BigDecimal = buildFghf(item, ywlx)
    //国债利息
    val Fgzlx:BigDecimal = BigDecimal(0)
    //回购收益
    val Fhggain:BigDecimal = BigDecimal(0)
    //风险金
    val Ffxj:BigDecimal = buildFfxj(item, ywlx)
    //中间结果
    val FeeTemp:BigDecimal = buildFeeTemp(item:Row)
    //清算标志
    val FQsbz:String = "N"
    //证券代码
    val ZQDM:String = trim(item.getAs[String]("zqdm1"))
    //交易方式
    val FJYFS:String = "PT"
    //	审核状态
    val Fsh:String = "1"
    //	制作人
    val FZZR:String = ""
    //审核人
    val FCHK:String = ""
    //	指令号
    val fzlh:String = "0"
    //	投资标志
    val ftzbz:String = ""
    //券商过户费
    val FQsghf:String	= "0"
    //股东代码
    val fgddm	= trim(item.getAs[String]("zqzh"))
    //交易标志
    val Fjybz	= ""
    //结算方式
    val ISRTGS = "1"
    //结算会员
    val FPARTID	= ""
    //合同序号
    val FHTXH = ""
    //初始合同序号
    val FCSHTXH	= ""
    //融资利率
    val FRZLV = "0"
    //初始购回期限
    val FCSGHQX =	"0"
    //数据来源
    val FSJLY = ""
    //币种
    val Fbz = "RMB"

    //金额、佣金、卖实收金额、其他费用
    val (fje,fsssje,fQTF) = buildFje(item,fbs,FeeTemp,Fghf,Fjsf,Fzgf,Fyhs,Ffxj,gddmParamMap,yjlvMap,ffyffsMap)

    Hzjkqs(
      FSETID,
      Fdate,
      FinDate,
      FZqdm,
      FSzsh,
      Fjyxwh,
      fbs,
      fje.doubleValue().toString,
      Fsl.doubleValue().toString,
      "",//佣金
      Fjsf.doubleValue().toString,
      Fyhs.doubleValue().toString,
      Fzgf.doubleValue().toString,
      Fghf.doubleValue().toString,
      Ffxj.doubleValue().toString,
      fQTF.doubleValue().toString,
      Fgzlx.doubleValue().toString,
      Fhggain.doubleValue().toString,
      fsssje.doubleValue().toString,
      FZqbz,
      Fywbz,
      Fjybz,
      FQsbz,
      ZQDM,
      FJYFS,
      Fsh,
      FZZR,
      FCHK,
      fzlh,
      ftzbz,
      FQsghf,
      fgddm,
      ISRTGS,
      FPARTID,
      FHTXH ,
      FCSHTXH,
      FRZLV,
      FCSGHQX,
      FSJLY,
      Fbz,
      "",
      "",
      "",
      "",
      "")
  }

  /**
    * 佣金计算结果
    * @param item
    * @param bdGddmParamMap
    * @param bdZqdm1ValueMap
    * @param bdGddmFJJLxLBMap
    * @param bdxwhZslMap
    * @param bdZqdm1ZS2Map
    * @param bdYjlvMap
    * @param bdFFyffsMap
    * @return
    */
  def yJcalculationResult(item: Row,
                        bdGddmParamMap:Broadcast[scala.collection.Map[String,String]],
                        bdZqdm1ValueMap:Broadcast[scala.collection.Map[String,String]],
                        bdGddmFJJLxLBMap:Broadcast[scala.collection.Map[String,String]],
                        bdxwhZslMap:Broadcast[scala.collection.Map[String,String]],
                        bdZqdm1ZS2Map:Broadcast[scala.collection.Map[String,String]],
                        bdYjlvMap:Broadcast[scala.collection.Map[String,String]],
                        bdFFyffsMap:Broadcast[scala.collection.Map[String,String]]):YjModel = {

    val gddmParamMap = bdGddmParamMap.value
    val zqdm1Map = bdZqdm1ValueMap.value
    val gddmfjjlxlbMap = bdGddmFJJLxLBMap.value
    val xwhzslMap = bdxwhZslMap.value
    val zqdm1zs2Map = bdZqdm1ZS2Map.value
    val yjlvMap = bdYjlvMap.value
    val ffyffsMap = bdFFyffsMap.value

    val gddm = trim(item.getAs[String]("zqzh"))
    val xwh = trim(item.getAs[String]("xwh1"))
    val wbhl: String = trim(item.getAs[String]("wbhl"))
    val wbje: String = item.getAs[Double]("wbje").doubleValue().toString
    val ywlx:String = item.getAs[String]("ywlx")

    //配置参数
    val paramstr = gddmParamMap.getOrElse(gddm, "")
    val GgTyWwCcLfS: String = getValueFromStr(paramstr, "GGTYWWCCLFS")
    val blnGgtCbXqr: String = getValueFromStr(paramstr, "BLNGGTCBXQR")
    val yjqsjexhvxyj: String = getValueFromStr(paramstr, "YJQSJEXHVXYJ")
    val yjblws: String = getValueFromStr(paramstr, "YJBLWS")
    val blnBhYj: String = getValueFromStr(paramstr, "BLNBHYJ")

    //深圳上海
    val FSzsh:String= "F"
    //证券标志
    val FZqbz:String = getFZqbz(item)

    //港股通一级费用
    val ffyffs = ffyffsMap.getOrElse(xwh + "_" + gddm + "_" + FZqbz, "")
    val GGTFY: Boolean = if (getValueFromStr(ffyffs, "GGTFY").equals("0")) true else false
    val GGTYHS: Boolean = if (getValueFromStr(ffyffs, "GGTYHS").equals("0")) true else false
    val GGTJYZF: Boolean = if (getValueFromStr(ffyffs, "GGTJYZF").equals("0")) true else false
    val GGTJYF: Boolean = if (getValueFromStr(ffyffs, "GGTJYF").equals("0")) true else false
    val GGTJYXTSYF: Boolean = if (getValueFromStr(ffyffs, "GGTJYXTSYF").equals("0")) true else false
    val GGTGFJSF: Boolean = if (getValueFromStr(ffyffs, "GGTGFJSF").equals("0")) true else false


    //获取佣金参数信息
    val fsetCode: String = getValueFromStr(paramstr, "FSETCODE")
    val yjxwhkey = FZqbz + "_" + FSzsh + "_" + xwh
    val yjgddmkey = FZqbz + "_" + FSzsh + "_" + gddm
    var yjstr = yjlvMap.getOrElse(yjxwhkey, "")
    if (strIsNull(yjstr)) yjstr = yjlvMap.getOrElse(yjgddmkey, "")

    //佣金利率
    var fjlv: BigDecimal = null
    //最小佣金
    var YjMin: BigDecimal = null
    //折扣率
    var YjZk: BigDecimal = null

    if (!strIsNull(yjstr)) {
      fjlv = BigDecimal(yjstr.split("_")(0))
      YjMin = BigDecimal(yjstr.split("_")(1))
      YjZk = BigDecimal(yjstr.split("_")(2))
    }

    //佣金
    var Fyj: BigDecimal = null
    //金额
    var Fje: BigDecimal = null
    //卖实收金额
    var Fsssje: BigDecimal = null

    if (strIsNull(yjstr)) {
      Fyj = BigDecimal(0)
    }

    //日期
    val Fdate:String = getFdate(item)
    //日期
    val FinDate:String = item.getAs[String]("qsrq")
    //证券代码
    val FZqdm:String = getFzqdm(item, zqdm1Map)
    //交易席位号
    val Fjyxwh:String = getFjyxwh(item)
    //买卖方向
    val fbs:String = buildFbs(item, ywlx)
    //经手费
    val Fjsf:BigDecimal = buildFjsf(item, ywlx)
    //印花税
    val Fyhs:BigDecimal = buildFyhs(item, ywlx)
    //征管费
    val Fzgf:BigDecimal = buildFzgf(item, ywlx)
    //过户费
    val Fghf:BigDecimal = buildFghf(item, ywlx)
    //风险金
    val Ffxj:BigDecimal = buildFfxj(item, ywlx)
    //业务标志
    val Fywbz:String = getFywbz(item, gddmfjjlxlbMap,xwhzslMap,zqdm1zs2Map)
    //证券代码
    val ZQDM:String = trim(item.getAs[String]("zqdm1"))
    val FeeTemp:BigDecimal = buildFeeTemp(item:Row)

    var (fje,fsssfje,fQTF) = buildFje(item,fbs,FeeTemp,Fghf,Fjsf,Fzgf,Fyhs,Ffxj,gddmParamMap,yjlvMap,ffyffsMap)

    if (!strIsNull(yjstr)) {
      //计算佣金时 不区分blnGgtCbXqr==0 和blnGgtCbXqr==1， 公式均为：round(ABS(WBJE)*WBHL,2)
      val reFje = round(abs(wbje) * BigDecimal(wbhl), yjblws.toInt)
      Fyj = round(fmax(round(reFje * fjlv, 2), YjMin) * YjZk, 2)

      if (GGTFY) {
        Fyj = Fyj - FeeTemp - fQTF
      } else {
        if (GGTYHS) {
          Fyj = Fyj - Fyhs
        }
        if (GGTJYZF) {
          Fyj = Fyj - Fzgf
        }
        if (GGTJYF) {
          Fyj = Fyj - Fjsf
        }
        if (GGTJYXTSYF) {
          Fyj = Fyj - Fghf
        }
        if (GGTGFJSF) {
          Fyj = Fyj - Ffxj
        }
      }
    } else {
      //佣金参数不存在时， 佣金为0
      Fyj = BigDecimal(0)
    }

    if(blnBhYj.equals("1")) {
      if (fbs.equals("B")) {
        fsssfje=fsssfje-Fyj
      }
      if (fbs.equals("S")) {
        fsssfje=fsssfje+Fyj
      }
    }
    Fsssje = fsssfje

    if(ywlx.equals("H54") || ywlx.equals("H55")|| ywlx.equals("H60")|| ywlx.equals("H63")
      || ywlx.equals("H64")|| ywlx.equals("H65")|| ywlx.equals("H67")) {
      Fyj = BigDecimal(0)
    }

    YjModel(Fdate, FinDate, FZqdm,Fjyxwh,FZqbz,Fywbz,ZQDM,fbs,Fyj.doubleValue().toString,Fsssje.doubleValue().toString)
  }

  /**
    * 业务标志
    * @param item
    * @param gddmfjjlxlbMap
    * @param xwhzslMap
    * @param zqdm1zs2Map
    * @return
    */
  def getFywbz(item:Row, gddmfjjlxlbMap:scala.collection.Map[String,String],
               xwhzslMap:scala.collection.Map[String,String],
               zqdm1zs2Map:scala.collection.Map[String,String]):String = {
    var Fywbz = ""
    val ywlx:String = trim(item.getAs[String]("ywlx"))
    val qsbz:String = trim(item.getAs[String]("qsbz"))
    val ggdm = trim(item.getAs[String]("zqzh"))
    val zqdm1 = trim(item.getAs[String]("zqdm1"))
    val xwh = trim(item.getAs[String]("xwh1"))

    val fjjxb = gddmfjjlxlbMap.getOrElse(ggdm, "-1_-1")
    val fjjlx = fjjxb.split("_")(0)
    val fjjlb = fjjxb.split("_")(1)

    val zsl = xwhzslMap.getOrElse(xwh, "")
    val zs2 = zqdm1zs2Map.getOrElse(zqdm1, "")
    if (ywlx.equals("H01")) {
      if (qsbz.equals("H01")) {
        val boo11 = fjjlx.equals("0")
        val bool2 = fjjlb.equals("1") || fjjlb.equals("5") || fjjlb.equals("7")
        val bool3 = !strIsNull(zsl) && zsl.equals("1")
        val bool4 = !strIsNull(zs2) && zs2.equals("1")
        if (boo11 && bool2 && (bool3||bool4)) {
          Fywbz = "ZS"
        }else {
          Fywbz = "PT"
        }
      }
      if (qsbz.equals("H02")) {
        Fywbz = "GGQZ"
      }

    } else if (ywlx.equals("H02")) {
      if (qsbz.equals("H03")) {
        Fywbz = "RGQZ"
      }

    }else if (ywlx.equals("H54") || ywlx.equals("H55")) {
      Fywbz = "PXDZ"

    }else if (ywlx.equals("H60")) {
      Fywbz = "FY"

    }else if (ywlx.equals("H63")) {
      Fywbz = "GSSG"

    }else if (ywlx.equals("H64")) {
      Fywbz = "PG"

    }else if (ywlx.equals("H65")) {
      Fywbz = "GG"

    }else if (ywlx.equals("H67")) {
      Fywbz = "PXDZ"
    }
    Fywbz
  }

  /**
    * 证券标志
    * @param item
    * @return
    */
  def getFZqbz(item:Row): String = {
    val ywlx = trim(item.getAs[String]("ywlx"))
    val qsbz = trim(item.getAs[String]("qsbz"))
    var FZqbz:String = ""
    if (ywlx.equals("H01")) {
      if (qsbz.equals("H01")) {
        FZqbz = "GP"
      }
      if (qsbz.equals("H02")) {
        FZqbz = "QZ"
      }
    } else if (ywlx.equals("H02")) {
      if (qsbz.equals("H03")) {
        FZqbz = "QZ"
      }
    }else if (ywlx.equals("H54") || ywlx.equals("H55")) {
      FZqbz = "QY"
    }else if (ywlx.equals("H60")) {
      FZqbz = "GP"
    }else if (ywlx.equals("H63")) {
      FZqbz = "GP"
    }else if (ywlx.equals("H64")) {
      FZqbz = "QY"
    }else if (ywlx.equals("H65")) {
      FZqbz = "QY"
    }else if (ywlx.equals("H67")) {
      FZqbz = "QY"
    }
    FZqbz
  }

  /**
    * 获取证券代码
    * @param item
    * @param zqdm1Map
    * @return
    */
  def getFzqdm(item:Row, zqdm1Map:scala.collection.Map[String,String]):String={
    val zqdm1 = trim(item.getAs[String]("zqdm1"))
    zqdm1Map.getOrElse(zqdm1, "")
  }


  /**
    * 获取日期
    * @param item
    * @return
    */
  def getFdate(item:Row): String = {
    val ywlx:String = item.getAs[String]("ywlx")
    var qsrq = item.getAs[String]("qsrq")
    if (ywlx.equals("H54") || ywlx.equals("H55")) {
      qsrq = item.getAs[String]("jsrq")
    }
    qsrq
  }

  /**
    * 交易席位号
    * @param item
    * @return
    */
  def getFjyxwh(item:Row):String ={
    var xwh1 = trim(item.getAs[String]("xwh1"))
    if(xwh1.length>=5) {
      xwh1 = xwh1.substring(0,5)
    }else {
      xwh1 = "%05d".format(xwh1.toInt)
    }
    xwh1
  }

  /**
    * 提取出计算用的字段
    * @param jsmxdbfDF
    * @param spark
    * @return
    */
  def buildCalculationDF(jsmxdbfDF:DataFrame, spark:SparkSession,gddmParamRDD:RDD[(String,String)]):DataFrame={
    import spark.implicits._

    jsmxdbfDF.createOrReplaceTempView("jsmx_source_t")

    spark.sql("select YWLX,BDLX,QSRQ,JSRQ,XWH1, trim(ZQZH) ZQZH, ZQDM1,QSBZ,MMBZ,WBHL,CJBH," +
      " SQBH,SL,CJSL,WBJE,YHS,JYZF,JYF,SYF,JSF,QTJE,WBYSF,YSFJE from jsmx_source_t").createOrReplaceTempView("source_field_table")

    gddmParamRDD.toDF("gddm", "params").createOrReplaceTempView("zhzqParam_table")

    spark.sql(
      " select YWLX ywlx, BDLX bdlx, QSRQ qsrq, JSRQ jsrq, xwh1 xwh1, ZQZH zqzh, ZQDM1 zqdm1, " +
        "QSBZ qsbz, MMBZ mmbz, WBHL wbhl, CJBH cjbh, SQBH sqbh, SL sl, CJSL cjsl, WBJE wbje, " +
        "YHS yhs, JYZF jyzf, JYF jyf, SYF syf, JSF jsf, QTJE qtje, WBYSF wbysf, YSFJE ysfje, " +
        "t2.params paraminfos " +
        " from source_field_table t1 left outer join zhzqParam_table t2 on t1.ZQZH = t2.gddm"
    )
  }

  /**
    * 根据 xwh_gddm_fzqlb 计算各个类型的费用
    * @param xwhGddmPairRDD
    * @param spark
    * @param url
    * @param date
    * @param qsrq
    * @return
    */
  def buildFFYFS(xwhGddmPairRDD:RDD[(String,String)],spark:SparkSession,url:String,date:String, qsrq:String) = {
    import spark.implicits._
    val a117csxwfyRDD = loadA117CSXWFY(spark, url, date, "A001CSXWFY")

    a117csxwfyRDD.toDF().createOrReplaceTempView("a117csxwfy_table")

    xwhGddmPairRDD.toDF("xwh", "gddm").createOrReplaceTempView("xwh_gddm_table")

    val qsrqstr = formatDate2Str(qsrq, "yyyyMMdd", "yyyy-MM-dd hh:mm:ss")

    val csxwfyFilterDF = spark.sql(
      "select * from a117csxwfy_table where fstartdate <= '"+qsrqstr+"' AND fsh = 1 " +
        " AND FZQLB IN('GP', 'QZ') AND ffylb in ('GGTFY','GGTYHS','GGTJYZF','GGTJYF','GGTJYXTSYF', 'GGTGFJSF') "
    )
    csxwfyFilterDF.createOrReplaceTempView("csxwfy_filter_table")

    val ffyfsDF= spark.sql(
      "select t1.xwh, t1.gddm, t2.fzqlb, t2.ffylb, t2.ffyfs" +
        " from xwh_gddm_table t1 left join csxwfy_filter_table t2 " +
        " on (t1.xwh = t2.fqsxw or t1.gddm = t2.fqsxw) "
    )

    val ffyfsCostRDD = ffyfsDF.rdd.map { item =>
      val xwh = item.getAs[String]("xwh")
      val gddm = item.getAs[String]("gddm")
      val fzqlb = item.getAs[String]("fzqlb")
      val ffylb = item.getAs[String]("ffylb")
      val ffyfs = item.getAs[String]("ffyfs")
      (xwh+"_"+gddm+"_"+fzqlb, ffylb+"="+ffyfs)
    }.groupByKey().map { case (key, value) =>
      val sb = new mutable.StringBuilder()
      for (v <- value) {
        sb.append("|").append(v)
      }
      (key, sb.toString())
    }
    ffyfsCostRDD.collectAsMap()
  }

  /**
    * 佣金费率取数规则:套帐号+证券类别+证券市场+席位代码(股东代码)
    *
    * 转换佣金利率Map
    * @param spark
    * @param curl
    * @param cdate
    * @return
    */
  def buildYjlbMap(spark:SparkSession, curl:String, cdate:String) = {

    val yjlvRDD = spark.sparkContext.textFile(curl + cdate + "/" + "A001CSYJLV")
    val yjlvPairRDD = yjlvRDD.map { item =>
      val items = item.split(",")
      //套帐号
      val fsetCode = items(0)
      //证券类别
      val fzqlb = items(1)
      //证券市场
      val fszsh = items(2)
      //费用费率
      val fLv = items(3)
      //佣金最低值
      val fLvMin = items(4)
      //席位代码(股东代码)
      val fStr1 = items(6)
      //折扣率
      val flvzk = items(10)

      (fzqlb+"_"+fszsh+"_"+fStr1, fLv+"_"+fLvMin+"_"+flvzk)
    }

    yjlvPairRDD.distinct().collectAsMap()
  }

  /**
    * 获取判断ZS股票的数据
    * @param zqdm1ValueRDD (zqdm1, zqdm1对应的证券代码)
    * @param spark
    * @param url
    * @param date
    * @param qsrqStr
    * @return (zqdm1, zsl)
    */
  def buildExistsZSbyZqdm(zqdm1ValueRDD:RDD[(String,String)], spark:SparkSession, url:String, date:String, qsrqStr:String) = {
    import spark.implicits._

    zqdm1ValueRDD.toDF("zqdm1", "fzqdm").createOrReplaceTempView("zqdm1_fzqdm_table")

    val a117cstskmDF = loadA117CSTSKM(spark, url, date, "A001CSTSKM", date)
    a117cstskmDF.createOrReplaceTempView("cstskm_zqdm_table")

    val zqdmFzqdmDF = spark.sql(
      " select t1.zqdm1 zqdm1, t1.fzqdm fzqdm, t2.fzqdm fzqdm2" +
        " from zqdm1_fzqdm_table t1 left join cstskm_zqdm_table t2 on t1.fzqdm=t2.FZQDM"
    )

    val zs2RDD = zqdmFzqdmDF.rdd.map { item=>
      val zqdm1 = item.getAs[String]("zqdm1")
      val fzqdm2 = item.getAs[String]("fzqdm2")
      var sb = ""
      if(!strIsNull(fzqdm2)) sb = "1" else sb = "0"
      (zqdm1, sb)
    }
    zs2RDD.collectAsMap()
  }

  /**
    * 获取判断ZS股票的数据
    * gsdm是指数席位号：
    * select 1 from A117CsQsXw where fstartdate<=日期 and fsh=1 and (fxwlb='ZS'or fxwlb='ZYZS') AND fqsxw=gsdm
    * @param xwhFsetcodeRDD
    * @param spark
    * @param url
    * @param date
    * @param qsrqStr
    * @return (xwh, zsl)
    */
  def buildExistsZSbyGsdm(xwhFsetcodeRDD:RDD[(String,String)], spark:SparkSession, url:String, date:String, qsrqStr:String) = {
    import spark.implicits._
    xwhFsetcodeRDD.toDF("xwh","fsetcode").createOrReplaceTempView("xwh_oth_table")

    val loadCsQsXwDF = loadCsQsXw(spark, url, date, "CSQSXW")
    loadCsQsXwDF.createOrReplaceTempView("csqsxw_table")

    val csQsXwFilterDF = spark.sql(
      " select * from csqsxw_table " +
        " where FSTARTDATE <= '"+qsrqStr+"' and FSH=1 and FXWLB in ('ZS','ZYZS')")
    csQsXwFilterDF.createOrReplaceTempView("csqsxw_filter_table")


    val xwhJoinFqsxwDF = spark.sql(
      "select t1.xwh, t2.FQSXW zs from xwh_oth_table t1 " +
        " left join csqsxw_filter_table t2 ON t1.xwh = t2.FQSXW and t1.fsetcode = t2.FSETCODE")

    val xwhZslRDD = xwhJoinFqsxwDF.rdd.map{ item =>
      val xwh = item.getAs[String]("xwh")
      val zs = item.getAs[String]("zs")
      var sb = ""
      if(!strIsNull(zs)) sb = "1" else sb = "0"
      (xwh, sb)
    }
    xwhZslRDD.collectAsMap()
  }

  /**
    * 计算gddm和FJJLx 和 FJJLB的关系
    * @param gddmFsetCodeRDD(gddm, fsetCode)
    * @param spark
    * @param curl
    * @param cdate
    * @param qsrq
    * @return (gddm, FJJLx_FJJLB)
    */
  def buildGddmFJJLxLBRDD(gddmFsetCodeRDD:RDD[(String,String)], spark:SparkSession, curl:String, cdate:String, qsrq:String)={

    val lsetcssysjjRDD = loadLSetCsSysJj(spark, curl, cdate, "LSETCSSYSJJ")

    val gddmLXLBRDD = gddmFsetCodeRDD.map(i => (i._2, i._1)).leftOuterJoin(lsetcssysjjRDD).map { case (fsetCode, (gddm, fjjlxFjjlb)) =>

      val fjjlxlv = fjjlxFjjlb match {
        //没有获取到fjjlxlv时，统一处理成 “_”，避免后面splits 取值报下标问题
        case None => "-1_-1"
        case Some(v) => if(strIsNull(v)) "-1_-1" else v
      }
      (gddm, fjjlxlv)
    }
    gddmLXLBRDD.collectAsMap()
  }

  /**
    * 根据原始字段zqdm1计算证券代码
    * @param zqdm1PairRDD
    * @param hktzxxDF
    * @param spark
    * @param qsrq 业务日期
    * @return RDD[(zqdm1, zqdm1对应的证券代码)]
    */
  def buildZqdm1ValueRDD(zqdm1PairRDD:RDD[(String, Int)], hktzxxDF:DataFrame, spark:SparkSession, qsrq:String):RDD[(String,String)] = {
    import spark.implicits._
    zqdm1PairRDD.toDF("zqdm1","oth").createOrReplaceTempView("zqdm1_table")
    //加载并过滤通知信息文件数据
    hktzxxDF.createOrReplaceTempView("hktzxx_h10_table")

    val hktzzFzdm1DF = spark.sql(
      " select t1.zqdm1, ZQDM zqdm, COUNT(*) OVER(partition by zqdm1) zcount " +
        " from zqdm1_table t1 " +
        " left outer join hktzxx_h10_table t2 on t1.zqdm1 = t2.FZDM1 " +
        " where '"+qsrq+"' >= RQ1 and '"+qsrq+"' < RQ2")

    val hktzzFzdm2DF = spark.sql(
      " select t1.zqdm1, ZQDM zqdm, COUNT(*) OVER(partition by zqdm1) zcount " +
        " from zqdm1_table t1 " +
        " left outer join hktzxx_h10_table t2 on t1.zqdm1 = t2.FZDM2 " +
        " where '"+qsrq+"' >= RQ2")

    val hktzzFzdm1RDD = hktzzFzdm1DF.rdd.map(item=>
      (item.getAs[String]("zqdm1"), item.getAs[Long]("zcount")+"_"+item.getAs[String]("zqdm")))
    val hktzzFzdm2RDD = hktzzFzdm2DF.rdd.map(item =>
      (item.getAs[String]("zqdm1"), item.getAs[Long]("zcount")+"_"+item.getAs[String]("zqdm")))

    val zqdm1ValueRDD = zqdm1PairRDD.leftOuterJoin(hktzzFzdm1RDD).leftOuterJoin(hktzzFzdm2RDD).map { case (zqdm1, ((_, c1),c2)) =>
      var res = ""
      val cv1 = c1 match {
        case None => ""
        case Some(sl) => sl
      }

      val cv2 = c2 match {
        case None => ""
        case Some(s2) => s2
      }

      if (!cv1.equals("") && cv1.split("_")(0).toInt > 0) {
        res = "H"+cv1.split("_")(1)
      } else if(!cv2.equals("") && cv2.split("_")(0).toInt > 0){
        res = "H"+cv2.split("_")(1)
      } else {
        res = "H" + zqdm1
      }
      (zqdm1, res)
    }

    zqdm1ValueRDD

  }

  /**
    * 获取gddm对应的相关参数信息
    * @param gddmFsetCodeRDD
    * @param spark
    * @param curl
    * @param cdate
    * @return (gddm, params)
    */
  def builGddmKeyParamRDD(gddmFsetCodeRDD:RDD[(String,String)], spark:SparkSession, curl:String, cdate:String):RDD[(String,String)] = {
    var paramKeyMap = mutable.Map[String,String]()
    paramKeyMap.+=("ggTyWwCcLfS"->"港股通业务交易金额尾差处理方式")
      .+=("blnGgtCbXqr"->"港股通股票交易成本与费用(先确认成本)")
      .+=("blnFyCjBhMx"->"港股通按成交记录换算一级交易费用")
      .+=("blnFySqBhMx"->"港股通按申请编号汇总换算一级交易费用")
      .+=("blnYjCjBhMx"->"港股通按成交记录计算佣金")
      .+=("blnYjSqBhMx"->"港股通按申请编号汇总计算佣金")
      .+=("blnBhYj"->"实际收付金额包含佣金")
      .+=("yjqsjexhvxyj"->"港股通上海深圳佣金计算方法采用外币的清算金额乘外币汇率乘佣金费率")
      .+=("yjblws"->"港股通佣金保留位数")
    val paramTableMap = loadLVARLIST(spark, curl, cdate, "LVARLIST")

    val bparamTable = spark.sparkContext.broadcast(paramTableMap)
    val bparamKeys = spark.sparkContext.broadcast(paramKeyMap)

    val gddmParamRDD = gddmFsetCodeRDD.map { case (zhzq, fsetCode) =>
      val paramTable = bparamTable.value
      val paramkeys = bparamKeys.value


      val sb = new mutable.StringBuilder()
      sb.append("|FSETCODE="+fsetCode)
      for ((key, value) <- paramkeys) {
        val tablevalue = paramTable.get(fsetCode + value) match {
          case None => {
            if ("yjblws".equals(key)) { "2" } else { "0" }
          }
          case Some(sv) => sv
        }
        sb.append("|").append(key.toUpperCase).append("=").append(tablevalue)
      }
      (zhzq, sb.toString())
    }
    gddmParamRDD
  }

  /**
    * 获取股东代码和套账号的对应关系
    * @param gddmPairRDD
    * @param spark
    * @param curl
    * @param cdate
    */
  def buildGddmFsetCode(gddmPairRDD:RDD[(String,Int)], spark:SparkSession, curl:String, cdate:String):RDD[(String,String)]={
    val csgdzhGddmFsetCodeRDD = loadCsgdzh(spark, curl, cdate, "CSGDZH")

    val gddmfsetCodeRDD = gddmPairRDD.leftOuterJoin(csgdzhGddmFsetCodeRDD)
      .map{ case (zhzq, (_, fsetcode))=>
        val code = fsetcode match {
          case None => ""
          case Some(v) =>
            if(strIsNull(v)) "" else v
        }
        (zhzq, code)
      }
    gddmfsetCodeRDD
  }

  def buildZqdm1ValueMap(zqdm1ValueRDD:RDD[(String, String)])={
    zqdm1ValueRDD.collectAsMap()
  }

  /**
    * 计算买卖方向
    * @return
    */
  def buildFbs(item:Row, ywlx:String):String = {
    val mmbz = trim(item.getAs[String]("mmbz"))
    var mm = "S"
    if(ywlx.equals("H01") || ywlx.equals("H02")) {
      if(mmbz.equals("B")) {
        mm = "B"
      }
    }else if(ywlx.equals("H54") || ywlx.equals("H55") || ywlx.equals("H63")
      || ywlx.equals("H67")) {
      mm = "S"
    } else if(ywlx.equals("H60") || ywlx.equals("H64") || ywlx.equals("H65")){
      mm = "S"
    }
    mm
  }

  /**
    * 计算数量
    * @param item
    * @return
    */
  def buildFsl(item:Row, ywlx:String):BigDecimal = {
    val cjsl:String = item.getAs[Double]("cjsl").doubleValue().toString
    val sl:String = item.getAs[Double]("sl").doubleValue().toString
    var fsl = BigDecimal(0)
    if (ywlx.equals("H01") || ywlx.equals("H02") || ywlx.equals("H54") || ywlx.equals("H55")) {
      fsl = round(abs(cjsl),2)
    } else if(ywlx.equals("H60") || ywlx.equals("H67")) {
      fsl = BigDecimal(0)
    } else if(ywlx.equals("H64") || ywlx.equals("H63") || ywlx.equals("H65")) {
      fsl = round(abs(sl),2)
    }
    fsl
  }

  /**
    * 计算经手费
    * @param item
    * @param ywlx
    * @return
    */
  def buildFjsf(item:Row, ywlx:String):BigDecimal ={
    var fjsf = BigDecimal(0)
    val jyf:String = item.getAs[Double]("jyf").doubleValue().toString
    val wbhl:String = trim(item.getAs[String]("wbhl"))
    if(ywlx.equals("H01") || ywlx.equals("H02") || ywlx.equals("H54") || ywlx.equals("H55")
      || ywlx.equals("H64") || ywlx.equals("H65") || ywlx.equals("H67")) {
      fjsf = round(abs(jyf)*BigDecimal(wbhl),2)
    } else if(ywlx.equals("H60") || ywlx.equals("H63")) {
      fjsf = BigDecimal(0)
    }
    fjsf
  }

  /**
    * 计算印花税
    * @param item
    * @param ywlx
    * @return
    */
  def buildFyhs(item:Row, ywlx:String):BigDecimal={
    val wbhl:String = trim(item.getAs[String]("wbhl"))
    val yhs:String = item.getAs[Double]("yhs").doubleValue().toString
    var fyhs = BigDecimal(0)

    if (ywlx.equals("H01") || ywlx.equals("H02") || ywlx.equals("H54") || ywlx.equals("H55")
      || ywlx.equals("H64") || ywlx.equals("H65") || ywlx.equals("H67")  || ywlx.equals("H63")) {
      fyhs = round(abs(yhs)*BigDecimal(wbhl),2)
    } else if (ywlx.equals("H60")) {
      fyhs = BigDecimal(0)
    }
    fyhs
  }

  /**
    * 计算征管费
    * @param item
    * @param ywlx
    * @return
    */
  def buildFzgf(item:Row, ywlx:String):BigDecimal={
    val wbhl:String = trim(item.getAs[String]("wbhl"))
    val jyzf:String = item.getAs[Double]("jyzf").doubleValue().toString
    var fzgf = BigDecimal(0)

    if (ywlx.equals("H01") || ywlx.equals("H02") || ywlx.equals("H54") || ywlx.equals("H55")
      || ywlx.equals("H64") || ywlx.equals("H65") || ywlx.equals("H67")) {
      fzgf = round(abs(jyzf)*BigDecimal(wbhl),2)
    } else if (ywlx.equals("H60") || ywlx.equals("H63")) {
      fzgf = BigDecimal(0)
    }
    fzgf
  }

  /**
    * 计算过户费
    * @param item
    * @param ywlx
    * @return
    */
  def buildFghf(item:Row, ywlx:String):BigDecimal ={
    val syf:String = item.getAs[Double]("syf").doubleValue().toString
    val wbhl:String = trim(item.getAs[String]("wbhl"))
    var fghf = BigDecimal(0)

    if (ywlx.equals("H01") || ywlx.equals("H02") || ywlx.equals("H54") || ywlx.equals("H55")
      || ywlx.equals("H64") || ywlx.equals("H65") || ywlx.equals("H67")) {
      fghf = round(abs(syf)*BigDecimal(wbhl),2)
    } else if (ywlx.equals("H60") || ywlx.equals("H63")) {
      fghf = BigDecimal(0)
    }
    fghf
  }

  /**
    * 计算风险金
    * @param item
    * @param ywlx
    * @return
    */
  def buildFfxj(item:Row, ywlx:String):BigDecimal = {
    var ffxj = BigDecimal(0)
    val wbhl:String = trim(item.getAs[String]("wbhl"))
    val jsf:String = item.getAs[Double]("jsf").doubleValue().toString

    if (ywlx.equals("H01") || ywlx.equals("H02") || ywlx.equals("H54") || ywlx.equals("H55")
      || ywlx.equals("H64") || ywlx.equals("H65") || ywlx.equals("H67")) {
      ffxj = round(abs(jsf)*BigDecimal(wbhl),2)
    } else if (ywlx.equals("H60") || ywlx.equals("H63")) {
      ffxj = BigDecimal(0)
    }
    ffxj
  }

  def buildFeeTemp(item:Row):BigDecimal={
    val jsf:String = item.getAs[Double]("jsf").doubleValue().toString
    val syf:String = item.getAs[Double]("syf").doubleValue().toString
    val jyzf:String = item.getAs[Double]("jyzf").doubleValue().toString
    val yhs:String = item.getAs[Double]("yhs").doubleValue().toString
    val jyf:String = item.getAs[Double]("jyf").doubleValue().toString
    val wbhl:String = trim(item.getAs[String]("wbhl"))
    round((abs(syf)+abs(jsf)+abs(jyzf)+abs(yhs)+abs(jyf))*BigDecimal(wbhl),2)
  }

  /**
    *  计算 金额、佣金、卖实收金额、其他费用
    * @param item   ROW 对象
    * @param fbs    买卖方向
    * @param FeeTemp  中间费用结果
    * @param Fghf     过户费
    * @param Fjsf   经手费
    * @param Fzgf   征管费
    * @param Fyhs   印花税
    * @param Ffxj   风险金
    * @param gddmParamMap
    * @param yjlvMap      佣金map  (套帐号_证券类别_证券市场_席位代码(股东代码), 费用费率_佣金最低值_折扣率)
    * @param ffyffsMap    各个费用map (xwh+"_"+gddm+"_"+fzqlb 费用1=0_费用2=1)
    * @return
    */
  def buildFje(item:Row,
               fbs:String,
               FeeTemp:BigDecimal,
               Fghf:BigDecimal,
               Fjsf:BigDecimal,
               Fzgf:BigDecimal,
               Fyhs:BigDecimal,
               Ffxj:BigDecimal,
               gddmParamMap:scala.collection.Map[String,String],
               yjlvMap:scala.collection.Map[String,String],
               ffyffsMap:scala.collection.Map[String,String]):(BigDecimal,BigDecimal,BigDecimal)= {

    val wbhl: String = trim(item.getAs[String]("wbhl"))
    val ysfje: String = item.getAs[Double]("ysfje").doubleValue().toString
    val wbje: String = item.getAs[Double]("wbje").doubleValue().toString
    val gddm = trim(item.getAs[String]("zqzh"))

    //配置参数
    val paramstr = gddmParamMap.getOrElse(gddm, "")
    val GgTyWwCcLfS: String = getValueFromStr(paramstr, "GGTYWWCCLFS")
    val blnGgtCbXqr: String = getValueFromStr(paramstr, "BLNGGTCBXQR")
    val yjqsjexhvxyj: String = getValueFromStr(paramstr, "YJQSJEXHVXYJ")
    val yjblws: String = getValueFromStr(paramstr, "YJBLWS")
    val blnBhYj: String = getValueFromStr(paramstr, "BLNBHYJ")

    //金额
    var Fje: BigDecimal = null
    //卖实收金额
    var fsssfje: BigDecimal = BigDecimal(0)
    //其他费用
    var FQTF: BigDecimal = null

    if (blnGgtCbXqr.equals("0")) {
      if (fbs.equals("B")) {
        fsssfje = rounddown(ysfje, 2)
        Fje = (-fsssfje) - (FeeTemp)
        FQTF = FeeTemp - Fghf - Fjsf - Fzgf - Fyhs - Ffxj

        if (GgTyWwCcLfS.equals("1")) {
          Fje = Fje + FQTF
          FQTF = 0
        }
      }
      if (fbs.equals("S")) {
        fsssfje = rounddown(ysfje, 2)
        Fje = fsssfje + FeeTemp
        FQTF = FeeTemp - Fghf - Fjsf - Fzgf - Fyhs - Ffxj

        if (GgTyWwCcLfS.equals("1")) {
          Fje = Fje - FQTF
          FQTF = 0
        }
      }

    }

    if (blnGgtCbXqr.equals("1")) {
      if (fbs.equals("B")) {
        fsssfje = rounddown(ysfje, 2)
        Fje = round(abs(wbje) * BigDecimal(wbhl), 2)
        FQTF = abs(fsssfje) - Fje - Fghf - Fjsf - Fzgf - Fyhs - Ffxj

        if (GgTyWwCcLfS.equals("1")) {
          Fje = Fje + FQTF
          FQTF = 0
        }
      }
      if (fbs.equals("S")) {
        fsssfje = rounddown(ysfje, 2)
        Fje = round(abs(wbje) * BigDecimal(wbhl), 2)
        FQTF = Fje - abs(fsssfje) - Fghf - Fjsf - Fzgf - Fyhs - Ffxj

        if (GgTyWwCcLfS.equals("1")) {
          Fje = Fje - FQTF
          FQTF = 0
        }
      }
    }

    if (yjqsjexhvxyj.equals("1")) {
      Fje = round(abs(wbje) * BigDecimal(wbhl), yjblws.toInt)
    }

    (Fje,fsssfje,FQTF)
  }

  def getValueFromStr(sourceStr: String, filename:String):String={
    val items = sourceStr.split("\\|")
    var result = ""
    for (item <- items) {
      if(!strIsNull(item)&&item.split("=").length==2) {
        if (filename.equals(item.split("=")(0))) {
          result = item.split("=")(1)
          return result
        }
      }
    }
    result
  }

  def mergeFileDF(spark: SparkSession, listFiles:ListBuffer[String]) = {
    val listDf = new mutable.ListBuffer[DataFrame]

    for (filename <- listFiles) {
      listDf += BasicUtils.readCSV(filename, spark, true)
    }
    var jsmxdbfDF = listDf(0)

    for (i <- 1 to listDf.length-1) {
      jsmxdbfDF = jsmxdbfDF.union(listDf(i))
    }
    jsmxdbfDF
  }

  def filterYWLX(spark: SparkSession, listFiles:ListBuffer[String]) = {
    val jsmxdbfDF = mergeFileDF(spark,listFiles)

    jsmxdbfDF.createOrReplaceTempView("hk_jsmx_table")
    spark.sql("select * from hk_jsmx_table where YWLX in ('H01','H02','H54','H55','H60','H63','H64','H65','H67')")
  }

  def loadInitParam(args: Array[String]):(String,String,String,String)={
    if(args.length < 1) {
      throw new Exception("args 参数不完整")
    }

    val currentDate = args(0)
    val hadoopRpcPath = "hdfs://192.168.102.120:8020"
    val commonUrl = "hdfs://192.168.102.120:8020/yss/guzhi/basic_list/"
    val ggtHadoopPath = s"/yss/guzhi/interface/${currentDate}/hkjsmx"

    (commonUrl, currentDate,ggtHadoopPath,hadoopRpcPath)
  }

  def getCurrentDate():String = {
    val now:Date = new Date()
    val dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val date = dateFormat.format(now)
    date
  }

  /**
    * @param spark
    * @param commonUrl
    * @param date
    * @param fileName
    * @return RDD[(GDDM, FSETCODE)]
    */
  def loadCsgdzh(spark:SparkSession, commonUrl:String, date:String, fileName:String)={
    val csgdzhRDD = spark.sparkContext.textFile(commonUrl + date + "/" + fileName)
    val csgdzhPairRDD = csgdzhRDD.map{ item=>
      val items = item.split(",")
      (trim(items(0)), trim(items(5)))
    }
    csgdzhPairRDD.distinct()
  }

  /**
    * 查询各种费用的信息
    * @param spark
    * @param commonUrl
    * @param date
    * @param fileName
    * @return
    */
  def loadA117CSXWFY(spark:SparkSession, commonUrl:String, date:String, fileName:String)= {
        val csgdzhRDD = spark.sparkContext.textFile(commonUrl + date + "/" + fileName)

        val csgdzhPairRDD = csgdzhRDD.map { item =>
          val items = item.split(",")
          CsxwfyModel(trim(items(0)), trim(items(1)), trim(items(2)), trim(items(3)), trim(items(4)),
            trim(items(5)), trim(items(6)), trim(items(7)), trim(items(8)))
        }
    csgdzhPairRDD
  }

  /**
    * Hktzxx文件数据
    * @param spark
    * @return
    */
  def getHktzxxDF(spark: SparkSession, listFiles:ListBuffer[String]) = {

    import spark.implicits._
    if (listFiles.length == 0) {
      Seq(
        ("", "", "", "", "")
      ).toDF("FZDM1", "FZDM2", "RQ1", "RQ2", "ZQDM")
    } else {
      mergeFileDF(spark, listFiles).createOrReplaceTempView("hk_tzxx_table")

      spark.sql("select FZDM1,FZDM2,RQ1,RQ2,ZQDM from hk_tzxx_table where TZLB='H10'")
    }
  }

  /**
    *    select 1 from CsQsXw where fstartdate<=日期
    *    and fsh=1 and fqsxw=gh 文件中的gsdm
    *    and fsetcode = "117"
    *    and fxwlb in ('ZS','ZYZS')
    *
    *    （gh文件中的gsdm字段在CsQsXw表中有数据 || zqdm字段在CsTsKm表中有数据）
    * @param spark
    * @param commonUrl
    * @param date
    * @param fileName
    * @return
    */
  def loadCsQsXw(spark:SparkSession, commonUrl:String, date:String, fileName:String)={
    val allCsQsXwRDD = spark.sparkContext.textFile(commonUrl + date + "/" + fileName)
    import spark.implicits._
    val allCsQsXwDF = allCsQsXwRDD.map{ item =>
      val items = item.split(",")
      CsqsxwModel(items(0),items(1),items(2),items(3),items(4),items(5),items(6),items(7),items(8),items(9))
    }.toDF()
    allCsQsXwDF
  }

  /**
    * zqdm 是维护的指数股票：
      select 1 from A117CsTsKm where fstartdate<=日期 and fsh=1 and fbz=3 and fzqdm=该zqdm

      zqdm 是维护的指标股票：
      select 1 from A117CsTsKm where fstartdate<=日期 and fsh=1 and fbz=2 and fzqdm=该zqdm
    * @param spark
    * @param commonUrl
    * @param date
    * @param fileName
    * @param qsrq
    * @return
    */
  def loadA117CSTSKM(spark:SparkSession, commonUrl:String, date:String, fileName:String, qsrq:String)={
    import spark.implicits._
    val cstskmRDD = spark.sparkContext.textFile(commonUrl + date + "/" + fileName)
    val cstskmPairRDD = cstskmRDD.map{ item=>
      val items = item.split(",")
      (trim(items(0)), trim(items(1)), trim(items(2)), trim(items(3)), trim(items(4)), trim(items(5)))
    }
    val cstskmPairDF = cstskmPairRDD.distinct()
      .toDF("FZQDM","FBZ", "FSH", "FZZR", "FCHK", "FSTARTDATE")

    cstskmPairDF.createOrReplaceTempView("cstskm_table")

    val resultDF = spark.sql(
      "select * from cstskm_table where FSTARTDATE <= '"+qsrq+"' and FSH=1 and (FBZ=2 or FBZ=3)"
    )
    resultDF
  }

  /**
    * 查询LSetCsSysJj表获取 (FSETCODE, FJJLX_FJJLB)的关系
    * @param spark
    * @param commonUrl
    * @param date
    * @param fileName
    * @return  (FSETCODE, FJJLX_FJJLB)  FJJLX_FJJLB  其中的字段可能为空
    */
  def loadLSetCsSysJj(spark:SparkSession, commonUrl:String, date:String, fileName:String)={
    val csSysJjRDD = spark.sparkContext.textFile(commonUrl + date + "/" + fileName)
    val csSysJjPairRDD = csSysJjRDD.map { item =>
      val items = item.split(",")
      var item1 = trim(items(1))
      var item2 = trim(items(3))
      if (strIsNull(item1)) item1 = ""
      if (strIsNull(item2)) item2 = ""
      (trim(items(0)), item1 + "_" + item2)
    }
    csSysJjPairRDD.distinct()
  }

  def loadLVARLIST(spark:SparkSession, commonUrl:String, date:String, fileName:String)={
    val lvarlistRDD = spark.sparkContext.textFile(commonUrl + date + "/" + fileName)
    val resultMap = lvarlistRDD.map (item => (item.split(",")(0), item.split(",")(1))).collectAsMap()
    resultMap
  }

  def formatDate2Str(date:String, sourceFormat:String, targetFormat:String): String ={
    val sf = new SimpleDateFormat(sourceFormat)
    val tf = new SimpleDateFormat(targetFormat)
    tf.format(sf.parse(date))
  }

  def round(num: BigDecimal, length:Int):BigDecimal={
    val resultDecimal:BigDecimal = num.setScale(length, RoundingMode.HALF_UP)
    resultDecimal
  }

  def rounddown(num: String, length:Int):BigDecimal={
    val resultDecimal:BigDecimal = BigDecimal(num).setScale(length, RoundingMode.HALF_DOWN)
    resultDecimal
  }

  def abs(num:String):BigDecimal={
    val numDecimal = BigDecimal(num)
    numDecimal.abs
  }

  def abs(num:BigDecimal):BigDecimal={
    num.abs
  }

  def fmax(pre:BigDecimal, later:BigDecimal):BigDecimal = {
    if (pre > (later)) {
      pre
    }else {
      later
    }
  }

  def strIsNull(str: String):Boolean={
    if (null == str || str.equals("")) {
      return true
    }
    if(str.toLowerCase().equals("null")) {
      return true
    }
    return false
  }

  def getDirFileNames(filepath:File):ListBuffer[String] = {

    val list = new mutable.ListBuffer[String]
    val listFiles = filepath.listFiles()
    for (listFile <- listFiles) {
      if(listFile.isDirectory) {
        list ++= getDirFileNames(new File(listFile.getPath))
      }
      if(listFile.isFile && listFile.getName.contains("jsmx")) {
        list += listFile.getPath
      }
    }

    return list
  }

  def getHadoopFilesName(hadoopRpcPath:String, filePath:String, jxms:String, tzxx:String) = {
    val jxmslist = new mutable.ListBuffer[String]
    val hktzxxList = new mutable.ListBuffer[String]
    var fs:FileSystem = null
    try {
      val config:Configuration = new Configuration()
      config.set("fs.default.name", hadoopRpcPath)
      fs = FileSystem.get(new URI(hadoopRpcPath),config,"hadoop")

      //会递归找到所有的文件
      val listFiles: RemoteIterator[LocatedFileStatus]  = fs.listFiles(new Path(filePath), true)

      while(listFiles.hasNext()){
        val fileStatus:LocatedFileStatus = listFiles.next()
        if (fileStatus.getPath.getName.contains(jxms)) {
          jxmslist += fileStatus.getPath.toString
        }
        if (fileStatus.getPath.getName.contains(tzxx)) {
          hktzxxList += fileStatus.getPath.toString
        }
      }
      (jxmslist,hktzxxList)
    }catch {
      case e1:Exception => throw new Exception("读取hadoop文件出错")
    }

  }

  def trim(str:String):String={
    var str2:String = ""
    if(!strIsNull(str)){
      str2 = str.trim
    }
    str2
  }
}
