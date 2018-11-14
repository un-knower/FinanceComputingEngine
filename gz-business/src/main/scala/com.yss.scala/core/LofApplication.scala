package com.yss.scala.core

import com.yss.scala.core.ShghContants._
import com.yss.scala.dto.Hzjkqs
import com.yss.scala.util.{DateUtils, Util}
import org.apache.spark.sql.SparkSession

/**
  * @auther: wusong
  * @date: 2018/11/5
  * @version: 1.0.0
  * @desc: 上证lof申请
  * @目标数据: bgh
  */
object LofApplication {

  def main(args: Array[String]): Unit = {
    var findate = DateUtils.getToday(DateUtils.YYYYMMDD)
    if(args.size >= 1){
      findate = args(0)
    }
    execute(findate)
  }

  /** 加载基础表信息 */
  def loadTables(spark:SparkSession,finDate:String) = {
    val sc = spark.sparkContext
    /** 过滤基金信息表 */
    val loadCsjjxx = () =>{
      val csjjxxPath = Util.getDailyInputFilePath(TABLE_NAME_JJXX)
      val csjjxx = sc.textFile(csjjxxPath)
        .filter(row => {
          val fields = row.split(SEPARATE2)
          val fsh = fields(10)
          val fszsh = fields(8)
          val fzqlx = fields(9)
          val fstartdate = fields(14)
          if (FSH.equals(fsh) && SH.equals(fszsh)
            && "LOF".equals(fzqlx)
            && fstartdate.compareTo(finDate)<=0) true
          else false
        })
        .map(row => {
          val fields = row.split(SEPARATE2)
          fields(1) //zqdm
        }).collect()
      sc.broadcast(csjjxx)
    }

    /** 股东账号表csgdzh */
    val loadCsgdzhvalue = () => {
      //读取股东账号表，
      val csgdzhPath = Util.getDailyInputFilePath(TABLE_NAME_GDZH)
      val csgdzhMap = sc.textFile(csgdzhPath)
        .map(row => {
          val fields = row.split(SEPARATE2)
          (fields(0), fields(5))
        }).collectAsMap()
      sc.broadcast(csgdzhMap)
    }

    /** 加载资产信息表 lsetlist */
    val loadLsetlist = () => {
      val lsetlistPath = Util.getDailyInputFilePath(TABLE_NAME_ZCXX)
      val lsetlistMap = sc.textFile(lsetlistPath)
        .map(row => {
          val fields = row.split(SEPARATE2)
          val fsetid = fields(1) // 资产代码
          val fsetcode = fields(2) //套账号
          (fsetcode,fsetid)  //根据套账号获取资产代码
        })
        .collectAsMap()
      sc.broadcast(lsetlistMap)
    }

    (loadCsjjxx(), loadCsgdzhvalue(), loadLsetlist())
  }

  def execute(finDate:String): Unit = {
    /**
      * 进行日期的格式化，基础信息表日期是 yyyy-MM-dd,原始数据是 yyyyMMdd
      * 这里将原始数据转换成yyyy-MM-dd的格式
      * @return yyyy-MM-dd
      */
    val convertDate = (bcrq:String) => {
      val yyyy = bcrq.substring(0,4)
      val mm = bcrq.substring(4,6)
      val dd = bcrq.substring(6)
      yyyy.concat(SEPARATE3).concat(mm).concat(SEPARATE3).concat(dd)
    }
    val convertedfinDate = convertDate(finDate)

    val spark = SparkSession.builder()
      .appName("bgh")
      .master("local[*]")
      .getOrCreate()

    val sourcePath = Util.getInputFilePath(finDate+"/bgh")
    val sourceDataFrame = Util.readCSV(sourcePath,spark)

    //加载基础表数据
    val broadcastValues = loadTables(spark,convertedfinDate)

    val broadcastValue_csjjxxList = broadcastValues._1
    val broadcastValue_csgdzh = broadcastValues._2
    val broadcastValue_lsetlist = broadcastValues._3

    /** 获取资产代码 */
    val getFsetid = (gddm:String) => {
      val fsetcode = broadcastValue_csgdzh.value.getOrElse(gddm,DEFAULT_VALUE)
      broadcastValue_lsetlist.value.getOrElse(fsetcode,DEFAULT_VALUE)
    }

    /** 过滤原始数据 */
    val filteredRdd = sourceDataFrame.rdd.filter(row => {
      val bcrq = row.getAs[String](1)
      val cjbz = row.getAs[String](11)
      val bs = row.getAs[String](12)
      val zqdm = row.getAs[String](5)
      val isExists = broadcastValue_csjjxxList.value.contains(zqdm)
      if(bcrq.equals(finDate) && isExists &&
        (("B".equals(bs) && ("LFS".equals(cjbz) || "LFC".equals(cjbz))) ||
          ("S".equals(bs) && "LFR".equals(cjbz)))
      ) true else false
    })

    /** 转换结果数据 */
    val resultRdd = filteredRdd.map(row => {
      val bcrq = convertedfinDate  //格式是yyyy-MM-dd
      val cjbz = row.getAs[String](11)
      val bs = row.getAs[String](12)
      val zqdm = row.getAs[String](5)
      val gddm = row.getAs[String](0)
      val gsdm = row.getAs[String](3)
      val fsetid = getFsetid(gddm)
      val cjje = row.getAs[String](9)
      val cjsl = row.getAs[String](4)
      val fzqbz = "CWJJ"
      val fywbz = "LOFSSSQ"
      val fjybz = if("LFS".equals(cjbz)) "认购申请" else if("LFC".equals(cjbz)) "申购申请" else "赎回申请"
      val fhtxh = "D"+fsetid+finDate
      Hzjkqs(fsetid, bcrq,bcrq,zqdm,SH,gsdm,bs,cjje,cjsl
        ,"0","0","0","0","0","0","0","0","0",
        "CWJJ","LOFSSSQ","N","0",zqdm,"PT","1",
        " "," ","0"," ","0",gddm,fjybz,"1"," ",
        fhtxh," ","0","0","bgh","RMB",
        "","","","","")
    })
    import spark.implicits._
    // 将结果保存到mysql中
    Util.outputMySql(resultRdd.toDF(),"bgh")
    // 将结果保存到hdfs上
    val hfdsPath = Util.getOutputFilePath(finDate+"/bgh")
    Util.outputHdfs(resultRdd.toDF(),hfdsPath)
    spark.stop()
  }
}
