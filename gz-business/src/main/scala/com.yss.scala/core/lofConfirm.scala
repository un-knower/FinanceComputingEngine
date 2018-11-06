package com.yss.scala.core

import com.yss.scala.core.ShghContants._
import com.yss.scala.dto.Hzjkqs
import com.yss.scala.util.Util
import org.apache.spark.sql.SparkSession

/**
  * @auther: wusong
  * @date: 2018/11/5
  * @version: 1.0.0
  * @desc: 上证lof确认
  */
class lofConfirm {

  def main(args: Array[String]): Unit = {

  }

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

    /**基础信息表日期是 yyyy-MM-dd,原始数据是 yyyyMMdd，这里将原始数据转换成yyyy-MM-dd的格式*/
    val convertDate = (bcrq:String) => {
      val yyyy = bcrq.substring(0,4)
      val mm = bcrq.substring(4,6)
      val dd = bcrq.substring(6)
      yyyy.concat(SEPARATE3).concat(mm).concat(SEPARATE3).concat(dd)
    }

    val convertedfinDate = convertDate(finDate)

    val spark = SparkSession.builder()
      .appName("lofmxzf")
      .master("local[*]")
      .getOrCreate()

    val sourcePath = Util.getInputFilePath(finDate+"/lofmxzf")
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
      val bzsm = row.getAs[String](9)
      val rgbz = row.getAs[String](28)
      val qrrq = row.getAs[String](2)
      val qrfe = row.getAs[String](16)
      val zqdm = row.getAs[String](11)
      val isExists = broadcastValue_csjjxxList.value.contains(zqdm)
      if(qrrq.equals(finDate) && isExists && qrfe.compareTo("0") > 0 &&
        (("642".equals(bzsm)||"643".equals(bzsm)) ||
          ("641".equals(bzsm) && "1".equals(rgbz)))
      ) true else false
    })

    /** 转换结果数据 */
    val resultRdd = filteredRdd.map(row => {
      val fdate = convertedfinDate //格式是yyyy-MM-dd
      val finDate = row.getAs[String](4)
      val zqdm = row.getAs[String](11)
      val bzsm = row.getAs[String](9)
      val fbs = if("641".equals(bzsm)||"642".equals(bzsm)) "B" else "S"
      val fje = row.getAs[String](17)
      val fsl = row.getAs[String](16)
      val sxfy =  BigDecimal(row.getAs[String](19))
      val dlfy =  BigDecimal(row.getAs[String](20))
      val jsfy =  BigDecimal(row.getAs[String](21))
      val ghfy =  BigDecimal(row.getAs[String](22))
      val qtfy =  BigDecimal(row.getAs[String](23))
      val yhse =  BigDecimal(row.getAs[String](24))
      val otf2 =  BigDecimal(row.getAs[String](25))
      val fqtf = (sxfy + dlfy + jsfy + ghfy + qtfy + yhse + otf2).formatted("%.2f")
      val fgddm = row.getAs[String](13)
      val fjybz = if("641".equals(bzsm)) "认购确认" else if("642".equals(bzsm)) "申购确认" else "赎回确认"
      val fsetid = getFsetid(zqdm)
      val fhtxh = "D"+fsetid+finDate
      Hzjkqs(fsetid, fdate,finDate,zqdm,SH," ",fbs,fje,fsl
        ,"0","0","0","0","0","0","0","0","0",
        "CWJJ","LOFSSSQ","N",fqtf,zqdm,"PT","1",
        " "," ","0"," ","0",fgddm,fjybz,"1"," ",
        fhtxh," ","0","0","lofmxzf","RMB",
        "","","","","")
    })
    import spark.implicits._
    Util.outputMySql(resultRdd.toDF(),"lofmxzf")
    // 将结果保存到hdfs上
    val hfdsPath = Util.getoutputFilePath(finDate+"/lofmxzf")
    Util.outputHdfs(resultRdd.toDF(),hfdsPath)
  }
}
