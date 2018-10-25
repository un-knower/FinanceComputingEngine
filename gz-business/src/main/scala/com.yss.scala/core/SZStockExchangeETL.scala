package com.yss.scala.core

import java.text.SimpleDateFormat

import com.twitter.chill.java.IterableRegistrar
import com.yss.scala.dto.SZSEOriginalObj
import com.yss.scala.core.ShghContants.SEPARATE1
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.yss.scala.util.Util

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * author:：ChenYao
  * 需要字段：证券代码  公司席位  股东代码
  * 原始数据表：execution_aggr_F000995F0401_1_20180808(1).tsv
  * 席位表： CSQSXW   字段: FQSXW 公司席位  FXWLB 席位类别(指数席位)
  * 特殊参数表： A117CSTSKM   字段: FZqdm 证券代码  Fbz 业务标志 （2 指标股票 3 指数股票）
  * 参数表:LVARLIST 字段 FVARNAME  FVARVALUE
  *
  *
  */



object SZStockExchangeETL extends java.io.Serializable {
  def getFywbzAndFzqbz() {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SJSV5")
    val sc = new SparkContext(sparkConf)
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val path="C:/Users/hgd/Desktop/估值资料/execution_aggr_tgwid_1_20180124.tsv"

    val dateSplit=path.split("/")
    val dateSplit1= dateSplit(5).split("_")
    val fileDate= dateSplit1(4).substring(0,8)
    val sdf1=new SimpleDateFormat("yyyyMMdd")
    val parseDate1=sdf1.parse(fileDate)  //解析成date
    val dateTime1=parseDate1.getTime

 //   val exe = sc.textFile("C:/Users/hgd/Desktop/估值资料/execution_aggr_F000995F0401_1_20180808(2).tsv")
     val exe=sc.textFile("C:/Users/hgd/Desktop/估值资料/execution_aggr_tgwid_1_20180124(1).tsv") //C:/Users/hgd/Desktop/execution_aggr_tgwid_1_20180124.tsv
    /**
      *  1.读取原始数据表
      */
    import sparkSession.implicits._
    val exeDF = exe.map {
      x =>
        val par = x.split("\t")
        val FZQDM = par(5) // 证券代码 5
        val key = FZQDM
        (key, x)
    }.groupByKey()
    //oriTable.show()

    /**
      * 2.取数据表 CSQSXW,进行map,将FQSXW为key,FXWLB为value
      */
    val xwTable = sc.textFile("hdfs://nscluster/yss/guzhi/basic_list/20181010/CSQSXW")
    val xwValue = xwTable.map {
      x => {
        val value = x.split(",")
        val FQSXW = value(3) //席位号
        val FXWLB = value(4) //席位类别
        (FQSXW, FXWLB)
      }
    }.collectAsMap()

    /**
      * 3.读取Lvarlist
      *
      */
    val varList = sc.textFile("hdfs://nscluster/yss/guzhi/basic_list/20181010/LVARLIST")
    val varlistValue = varList.map {
      x => {
         val par=x.split(",")
        val FVARNAME = par(0) //参数名称
        val FVARVALUE = par(1) //是否开启
        (FVARNAME, FVARVALUE)
      }
    }.collectAsMap()
    /**
      * 4.读取A117cstskm
      *
      */
    val cstskm = sc.textFile("hdfs://nscluster/yss/guzhi/basic_list/20181010/A001CSTSKM")
    val cstskmValue = cstskm.map {
      x => {
        val par=x.split(",")
        val FZqdm = par(0) //证券代码
        val Fbz = par(1) // 业务标志
        (FZqdm, Fbz)
      }
    }.collectAsMap()
    /**
      * 5.读取LSetCsSysJj 这张表
      *
      */
    val LSETCSSYSJJ = sc.textFile("hdfs://nscluster/yss/guzhi/basic_list/20181010/LSETCSSYSJJ")
    val LSETCSSYSJJValue = LSETCSSYSJJ.map {
      x => {
        val value = x.split(",")
        val FSETCODE = value(0) //证券代码
        val FJJLX = value(1)
        (FSETCODE, FJJLX)
      }
    }.collectAsMap()

    /**
      *
      * 6.读取基金信息表
      */
    val CSJJXX = sc.textFile("hdfs://nscluster/yss/guzhi/basic_list/20181010/CSJJXX")//hdfs://nscluster/yss/guzhi/basic_list/20180917/CSJJXX
    val CSJJXXValue = CSJJXX.map {
      x => {
        val value = x.split(",")
        val FSCZQDM = value(0) //市场证券代码
        val  FSZSH= value(8) //市场
        val  FZQLX= value(9) //基金类型
        //日期
        val fSatrtDate=value(14)

        //将日期转化成时间戳形式
         val sdf=new SimpleDateFormat("yyyy-MM-dd")
         val parseDate=sdf.parse(fSatrtDate)  //解析成date
         val dateTime=parseDate.getTime
         val key=FSCZQDM+"_"+FZQLX
        (key, dateTime)
      }
    }.groupByKey().mapValues(item => { //分组完成后进行排序
      item.toArray.sortWith((str1, str2) => {
        str1.compareTo(str2) > 0  //从大到小排序
      })
    }).collectAsMap()

    /**
      * 7.读取股东账号
      *
      */

    val accountNumber=sc.textFile("hdfs://nscluster/yss/guzhi/basic_list/20181010/CSGDZH")

    val setCode= accountNumber.map{
      x=>{
        var par=x.split(",")
        val AccountId=par(0)
        val fsetcode=par(5)
        (AccountId,fsetcode)
      }
    }.collectAsMap()

    /**
      *读取 CSZQXX表
      *
      */
    val CSZQXX=sc.textFile("hdfs://nscluster/yss/guzhi/basic_list/20181010/CSZQXX")

    val fzqlb= CSZQXX.map{
      x=>{
        var par=x.split(",")
        val ZQDM=par(0)
        val FZQLB=par(11)
        (ZQDM,FZQLB)
      }
    }.collectAsMap()



    //将map进行广播
    val xwValues = sc.broadcast(xwValue)
    val varlistValues = sc.broadcast(varlistValue)
    val cstskmValues = sc.broadcast(cstskmValue)
    val LSETCSSYSJJValues = sc.broadcast(LSETCSSYSJJValue)
    val CSJJXXValues = sc.broadcast(CSJJXXValue)
    val setCodeValues = sc.broadcast(setCode)
    val fzqlbValues=sc.broadcast(fzqlb)

    //将原始数据,进行map,将key进行判断
    val result = exeDF.flatMap{
      case (key1,iterable) => {

        var execution=new  ListBuffer[SZSEOriginalObj]()


        for (func <- iterable) {
          //定义一个map
          val fzqbz = mutable.Map("fzqbz" -> "0")
          val fywbz = mutable.Map("fywbz" -> "0")
          val setCode = mutable.Map("setCode" -> "0")

          val text = func.split("\t")
          val LastPx = text(16)
          //成交价
          val LastQty = text(17) //成交数量
          val ReportingPBUID = text(3) //回报交易单元
          val key = text(5) //证券代码
          val TransactTime1 = text(9)
          val appId = text(2)
          val TransactTime = TransactTime1.substring(0, 8) //回报时间
          val Side = text(20) //买卖方向
          val sqbh = text(12) //申请编号
          val AccountID = text(21)
          if (appId == "052") {
            if (key.substring(0, 2).equals("00") || key.substring(0, 2).equals( "30")) {
              //判断fzqbz
              if (key.substring(0, 4).equals("0010") || key.substring(0, 5).equals("00119")) {
                fzqbz("fzqbz") = "CDRGP"

              } else {
                fzqbz("fzqbz") = "GP"

              }
              /**
                * 判断fywbz
                * 1.从席位表中根据席位字段取得value,默认值-1，如果value!=-1 ,并且value=ZS
                * 2.从特殊参数表 取得value, 默认值-1,如果value!=-1,并且value=3
                * 3.从参数表中取得117指数、指标股票按特殊科目设置页面处理 , 默认值为-1，如果value!=-1,并且value=1
                *
                */

              val FXWLB = xwValues.value.getOrElse(ReportingPBUID, -1) //PT
              val FARVALUE = varlistValues.value.getOrElse("117指数、指标股票按特殊科目设置页面处理", -1) //-1
              val Fbz = cstskmValues.value.getOrElse(key, -1) // -1
              val FJJLX = LSETCSSYSJJValues.value.getOrElse("117", -1) //0


              //进行第一个判断
              if (FARVALUE == 1 && (FXWLB.equals("ZS") || Fbz == 3)) {
                fzqbz("fywbz") = "ZS"
              } else if (FARVALUE == 1 && FXWLB.equals("ZS")) {
                fywbz("fywbz") = "ZB"
              } else if (FJJLX == 0 && (FJJLX == 1 || FJJLX == 5 || FJJLX == 7) && (FXWLB == "ZS" && FXWLB == "ZYZS") || (Fbz == 2 && Fbz == 3)) {
                fywbz("fywbz") = "ZS"
              } else if (FJJLX == 0 && (FJJLX == 2) && (Fbz == 2 && Fbz == 3)) {
                fywbz("fywbz") = "ZB"
              } else {
                fywbz("fywbz") = "PT"
              }

            } else if (key.substring(0, 3).equals("140")) {
              fzqbz("fzqbz") = "GP"
              fywbz("fywbz") = "DZYXPT"

            } else if (key.substring(0, 2).equals("10")) {


              if (key.substring(0, 3).equals("104") || key.substring(0, 3) .equals( "106") || key.substring(0, 3).equals( "105") ||
                key.substring(0, 3).equals("107") || key.substring(0, 3).equals("109")) {

                fzqbz("fzqbz") = "ZQ"
                fywbz("fywbz") = "DFZQ"

              } else if (key.substring(0, 4).equals("1016")|| key.substring(0, 4).equals("1017")) {
                fzqbz("fzqbz") = "XZ"
                fywbz("fywbz") = "QYZQXZ"
              } else if (key.substring(0, 4).equals("1086") || key.substring(0, 4).equals("1087") || key.substring(0, 4).equals( "1088") ||
                key.substring(0, 4).equals("1089")) {
                fzqbz("fzqbz") = "ZQ"
                fywbz("fywbz") = "JRZQ_ZCX"

              } else {
                fzqbz("fzqbz") = "ZQ"
                fywbz("fywbz") = "GZXQ"
              }


            } else if (key.substring(0, 2).equals(  "11") || key.substring(0, 3).equals( "133") || key.substring(0, 3).equals( "134") || key.substring(0, 3).equals(  "138")
              || key.substring(0, 3).equals( "148") || key.substring(0, 3).equals( "149")) {

              if (key.substring(0, 3).equals( "138") || key.substring(0, 3).equals( "139") || key.substring(0,3).equals("119")) {

                fzqbz("fzqbz") = "ZQ"
                fywbz("fywbz") = "ZCZQ"
              } else if (key.substring(0, 4).equals("1189") || key.substring(0, 4).equals("1151")) {
                fzqbz("fzqbz") = "ZQ"
                fywbz("fywbz") = "CJZQ"
              } else if (key.substring(0, 4).equals( "1174") || key.substring(0, 3).equals( "114")
                || key.substring(0, 3).equals( "118") || key.substring(0, 4).equals( "1170") ||
                key.substring(0, 4).equals("1171") || key.substring(0, 4).equals( "1172") || key.substring(0, 4).equals( "1173")) {
                fzqbz("fzqbz") = "ZQ"
                fywbz("fywbz") = "SMZQ"

              } else if ((key.substring(0, 3).equals( "112" )|| key.substring(0, 4).equals( "1175")
                || key.substring(0, 4).equals( "1176") || key.substring(0, 4).equals( "1177") ||
                key.substring(0, 4).equals( "1178") || key.substring(0, 4).equals( "1179") || key.substring(0, 3).equals( "148")
                ||
                key.substring(0, 3).equals( "149" )|| key.substring(0, 3).equals( "133") || key.substring(0, 3).equals( "134")) && fzqlbValues.value.getOrElse(key,"-1")!="可分离债券" && key.substring(0, 3) != "119"){
                  fzqbz("fzqbz") = "ZQ"
                  fywbz("fywbz") = "QYZQ"
              }else if(fzqlbValues.value.getOrElse(key,"-1").equals("可分离债券")){
                fzqbz("fzqbz") = "ZQ"
                fywbz("fywbz") = "FLKZZ"
              } else {
                fzqbz("fzqbz") = "ZQ"
                fywbz("fywbz") = "KZZ"
              }

            } else if (key.substring(0, 2).equals( "12")) {
              fzqbz("fzqbz") = "ZQ"
              fywbz("fywbz") = "KZZ"
            } else if (key.substring(0, 2).equals( "13")) {

              if (appId.substring(0, 3).equals( "010") || appId.substring(0, 3).equals( "020") || appId.substring(0, 3).equals( "050") ||
                appId.substring(0, 3).equals( "060")) {
                if (Side == "1") {
                  fzqbz("fzqbz") = "HG"
                  fywbz("fywbz") = "MRHG"
                } else if (Side == "2") {
                  fzqbz("fzqbz") = "HG"
                  fywbz("fywbz") = "MCHG"
                }
              }

            } else if (key.substring(0, 2).equals( "16")) {
              fzqbz("fzqbz") = "JJ"
              fywbz("fywbz") = "LOF"
            } else if (key.substring(0, 2) .equals("18")) {
              fzqbz("fzqbz") = "JJ"
              fywbz("fywbz") = "FBS"
            } else if (key.substring(0, 2).equals("03")) {
              if (key.substring(0, 3).toInt >= 30 && key.substring(0, 3).toInt <= 32) {
                //RGQZ
                fzqbz("fzqbz") = "QZ"
                fywbz("fywbz") = "RGQZ"
              } else if (key.substring(0, 3).toInt >= 38 && key.substring(0, 3).toInt <= 39) {
                fzqbz("fzqbz") = "QZ"
                fywbz("fywbz") = "RZQZ"
              }
            } else if (key.substring(0, 2).equals( "15")) {


              val dateLong = CSJJXXValues.value.get(key + "_" + "HB")

              if (dateLong.isDefined) {
                val jjDate = dateLong.get(0)

                if (key.substring(0, 3) .equals("159") && jjDate!=0 && dateTime1.toString >= jjDate.toString) {
                  fzqbz("fzqbz") = "JJ"
                  fywbz("fywbz") = "HBETF"
                }
              }
              if (key.substring(0, 4) .equals("1599")) {
                fzqbz("fzqbz") = "JJ"
                fywbz("fywbz") = "ETF"
              } else {
                fzqbz("fzqbz") = "JJ"
                fywbz("fywbz") = "LOF"

              }

            }

            val setCodeValue = setCodeValues.value.getOrElse(AccountID, "-1")
            if (!setCodeValue .equals( "-1")) {

              setCode("setCode") = setCodeValue
            }
            //将iterable进行for循环，将要的数据放到case calss中，将所有数据放到list中


            val Exe = SZSEOriginalObj(TransactTime, appId, ReportingPBUID, key, LastPx, LastQty, Side, AccountID, fileDate, sqbh, fzqbz("fzqbz"), fywbz("fywbz"), setCode("setCode"))
            execution.append(Exe)
          }
        }
        execution
      }
    }



     import sparkSession.implicits._
    result.toDF().write.format("jdbc")
      .option("url","jdbc:mysql://192.168.102.120:3306/JJCWGZ")
      .option("user","root")
      .option("password","root1234")
      .option("dbtable","sjsv5_etl_cy")
      .mode(SaveMode.Overwrite)
      .save()



   result.toDF().show()


    //  exeDF.createOrReplaceTempView("exeDF")
    // val oriTable= sparkSession.sql("select ReportingPBUID,SecurityID,AccountID from exeDF ")
    // xw.show()

    //select fvarvalue from lvarlist where fvarname='117指数、指标股票按特殊科目设置页面处理'

    //sparkSession.sql("select ReportingPBUID from exeDF inner join CSQSXW where exeDF.ReportingPBUID= CSQSXW.FQSXW ").show()
    //  sparkSession.sql("select *,(case   when  SecurityID  Like  concat('00','%')   then 'ZS' else 'ZB' end)flb  from exeDF")


    //sparkSession.sql("select *,(case   when  SecurityID  Like  concat('00','%')  And fvarvalue='1'  then 'ZS' else 'ZB' end)flb  from ( exeDF UNION ALL select fvarvalue from lvarlist where fvarname='4资产净值市值不统计摊余成本债券估值增值') as lvarlist").show()
    //sparkSession.sql("select fvarvalue from lvarlist where fvarname='117指数、指标股票按特殊科目设置页面处理'")


    /* sparkSession.sql("select * from" +
       " (select *,(case   when  SecurityID  Like  concat('00','%') then 'ZS' else 'ZB' end)flb  from exeDF ) as exeDF  " +
       "inner join CSQSXW where exeDF.ReportingPBUID= CSQSXW.FQSXW ")*/

  }


  def main(args: Array[String]): Unit = {
    getFywbzAndFzqbz() //处理业务标志和证券标志两个字段
  }
}
