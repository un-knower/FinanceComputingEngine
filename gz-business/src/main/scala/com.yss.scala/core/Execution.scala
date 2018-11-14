package com.yss.scala.core

import java.text.SimpleDateFormat

import com.yss.scala.dto._
import com.yss.scala.core.ExecutionContaints._
import com.yss.scala.util.BasicUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.math.BigDecimal.RoundingMode

/**
  * @author ChenYao
  * @version 2018/10/14
  *          业务描述： 深交所大宗交易估值核算
  *          原始文件：execution_aggr_tgwid_1_20180124.tsv
  *          目标数据库：SZSTOCK
  */
object Execution extends Serializable {

  def main(args: Array[String]): Unit = {

    getResult()
  }

  def getResult() = {
    val spark = SparkSession.builder().appName("SJSV5").master("local[*]").getOrCreate() //.master("local[*]")
    import spark.implicits._
    val csb = loadLvarlist(spark.sparkContext)
    val df = getFywbzAndFzqbz(spark, csb)
    doExec(df.toDF, csb)
  }

  /**
    * 进行ETF
    */

  def getFywbzAndFzqbz(spark: SparkSession, csb: Broadcast[collection.Map[String, String]]) = {
    val sc = spark.sparkContext
    val path = "C:/Users/hgd/Desktop/估值资料/execution_aggr_tgwid_2_20180124.tsv"
    val dateSplit1 = path.split("_")
    val fileDate = dateSplit1(4).substring(0, 8)
    val sdf1 = new SimpleDateFormat("yyyyMMdd")
    val parseDate1 = sdf1.parse(fileDate) //解析成date
    val dateTime1 = parseDate1.getTime
    val exe = sc.textFile(path) //C:/Users/hgd/Desktop/execution_aggr_tgwid_1_20180124.tsv

    /**
      *  1.读取原始数据表
      */
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
    val CSJYLVPath = BasicUtils.getDailyInputFilePath("CSJYLV")
    val xwTable = sc.textFile(CSJYLVPath)
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
    val LVARLISTPath = BasicUtils.getDailyInputFilePath("LVARLIST")
    val varList = sc.textFile(LVARLISTPath)
    val varlistValue = varList.map {
      x => {
        val par = x.split(",")
        val FVARNAME = par(0) //参数名称
        val FVARVALUE = par(1) //是否开启
        (FVARNAME, FVARVALUE)
      }
    }.collectAsMap()

    /**
      * 4.读取CSSYSTSKM  特殊科目表
      * key
      *
      */
    val A001CSTSKMPath = BasicUtils.getDailyInputFilePath("CSSYSTSKM")
    val cstskm = sc.textFile(A001CSTSKMPath)
    val cstskmValue = cstskm.map {
      x => {
        val par = x.split(",")
        val fsetid=par(1) //资产代码
        val FZqdm = par(2) //证券代码
        val Fbz = par(3) // 业务标志
        val key=fsetid+SEPARATE1+FZqdm
        (key, Fbz)
      }
    }.collectAsMap()
    /**
      * 5.读取LSetCsSysJj 这张表
      *
      */
    val LSETCSSYSJJPath = BasicUtils.getDailyInputFilePath("LSETCSSYSJJ")
    val LSETCSSYSJJ = sc.textFile(LSETCSSYSJJPath)
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

    val CSJJXXPath = BasicUtils.getDailyInputFilePath("CSJJXX")
    val CSJJXX = sc.textFile(CSJJXXPath)
    //hdfs://nscluster/yss/guzhi/basic_list/20180917/CSJJXX
    val CSJJXXValue = CSJJXX.map {
      x => {
        val value = x.split(",")
        val FSCZQDM = value(0) //市场证券代码
        val FSZSH = value(8) //市场
        val FZQLX = value(9) //基金类型
        //日期
        val fSatrtDate = value(14)

        //将日期转化成时间戳形式
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val parseDate = sdf.parse(fSatrtDate) //解析成date
        val dateTime = parseDate.getTime
        val key = FSCZQDM + "_" + FZQLX
        (key, dateTime)
      }
    }.groupByKey().mapValues(item => { //分组完成后进行排序
      item.toArray.sortWith((str1, str2) => {
        str1.compareTo(str2) > 0 //从大到小排序
      })
    }).collectAsMap()



    /**
      * 7.读取股东账号
      *
      */
    val CSGDZHPath = BasicUtils.getDailyInputFilePath("CSGDZH")
    val accountNumber = sc.textFile(CSGDZHPath )

    val setCode = accountNumber.map {
      x => {
        var par = x.split(",")
        val AccountId = par(0)
        val fsetcode = par(5) //套账号
        (AccountId, fsetcode)
      }
    }.collectAsMap()

    /**
      * 读取 CSZQXX表
      *
      */
    val CSZQXXPath = BasicUtils.getDailyInputFilePath("CSZQXX")
    val CSZQXX = sc.textFile(CSZQXXPath)
    val fzqlb = CSZQXX.map {
      x => {
        var par = x.split(",")
        val ZQDM = par(0)
        val FZQLB = par(11)
        (ZQDM, FZQLB)
      }
    }.collectAsMap()
    //将map进行广播
    val xwValues = sc.broadcast(xwValue)
    val varlistValues = sc.broadcast(varlistValue)
    val cstskmValues = sc.broadcast(cstskmValue)
    val LSETCSSYSJJValues = sc.broadcast(LSETCSSYSJJValue)
    val CSJJXXValues = sc.broadcast(CSJJXXValue)
    val setCodeValues = sc.broadcast(setCode)
    val fzqlbValues = sc.broadcast(fzqlb)

    //将原始数据,进行map,将key进行判断
    val result = exeDF.flatMap{
      case (key1,iterable) => {
        var execution=new  ListBuffer[ExecutionOriginalObj]()
        for (func <- iterable) {
          //定义一个map
          val fzqbz = mutable.Map("fzqbz" -> "0")
          val fywbz = mutable.Map("fywbz" -> "0")
          val fsetid = mutable.Map("fsetid" -> "0")

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
          var SH=""

          val fsetidValue = setCodeValues.value.getOrElse(AccountID, "-1")
      /*    if (!fsetidValue .equals( "-1")) {

            fsetid("fsetid") = fsetidValue
          }*/

         if (appId.equals("010") || appId.equals("120") || appId.equals("140") || appId.equals("130") || appId.equals("090") || appId.equals("029")|| appId.equals("020")) {
           SH="S"
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
              val Fbz = cstskmValues.value.getOrElse(fsetidValue+SEPARATE1+key, -1) // -1
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
                appId.substring(0, 3).equals( "060")   || appId.substring(0, 3).equals( "052") || appId.substring(0, 3).equals( "053") ||
                appId.substring(0, 3).equals( "061")   ) {
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

            //将iterable进行for循环，将要的数据放到case calss中，将所有数据放到list中
            val Exe = ExecutionOriginalObj(TransactTime, appId, ReportingPBUID, key, LastPx, LastQty, Side, AccountID, fileDate, sqbh, fzqbz("fzqbz"), fywbz("fywbz"), fsetidValue,SH)
            execution.append(Exe)
          }else if(appId.equals("050") || appId.equals("052") /*|| appId.equals("053")*/ || appId.equals("060") || appId.equals("061") ){
              SH="SDZ"
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
             val Fbz = cstskmValues.value.getOrElse(fsetidValue+SEPARATE1+key, -1) // -1
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
               appId.substring(0, 3).equals( "060")  || appId.substring(0, 3).equals( "052") || appId.substring(0, 3).equals( "053") ||
               appId.substring(0, 3).equals( "061") ) {
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

           //将iterable进行for循环，将要的数据放到case calss中，将所有数据放到list中
           val Exe = ExecutionOriginalObj(TransactTime, appId, ReportingPBUID, key, LastPx, LastQty, Side, AccountID, fileDate, sqbh, fzqbz("fzqbz"), fywbz("fywbz"), fsetidValue,SH)
           execution.append(Exe)
         }
       }
        execution
      }
    }
    import spark.implicits._
    result.toDF.show
    result
  }


  /** 加载公共参数表lvarlist
    * 返回值: 广播变量 key 参数  value : 0 1 是否开启
    *
    * */
  def loadLvarlist(sc: SparkContext) = {
    //公共的参数表
    val csbPath = BasicUtils.getDailyInputFilePath("LVARLIST")
    val csb = sc.textFile(csbPath)

    //将参数表转换成map结构
    val csbMap = csb.map(row => {
      val fields = row.split(SEPARATE2)
      val key = fields(0)
      val value = fields(1)
      (key, value)
    })
      .collectAsMap()
    sc.broadcast(csbMap)
  }


  /** 汇总然后进行计算 */
  def doExec(df: DataFrame, csb: Broadcast[collection.Map[String, String]]) = {

    val spark = SparkSession.builder().appName("SJSV5").master("local[*]").getOrCreate()

    val sc = spark.sparkContext


    /** 加载公共费率表和佣金表
      * 佣金：
      * key: 证券类别+市场+交易席位  value: 启动日期+利率+折扣+最低佣金
      * 费率：
      * key:证券类别+市场+套账号+利率类别  value:启动日期+利率+折扣
      *
      * */
    def loadFeeTables() = {
      //公共的费率表
      val flbPath = BasicUtils.getDailyInputFilePath("CSJYLV")
      val flb = sc.textFile(flbPath)
      //117的佣金利率表
      val yjPath = BasicUtils.getDailyInputFilePath("CSSYSYJLV")
      val yjb = sc.textFile(yjPath)

      //将佣金表转换成map结构
      val yjbMap = yjb.map(row => {
        val fields = row.split(SEPARATE2)
        val zqlb = fields(3) //证券类别
        val sh = fields(4) //市场
        val lv = fields(5) //利率
        val minLv = fields(6) //最低利率
        val startDate = fields(16) //启用日期
        //      val zch = row.get(15).toString // 资产
        val zk = fields(12) //折扣
        val fstr1 = fields(8) //交易席位/公司代码
        val fsetid=fields(1)

        val key =fsetid+ SEPARATE1+zqlb + SEPARATE1 + sh + SEPARATE1 + fstr1 //证券类别+市场+交易席位/公司代码
        val value = startDate + SEPARATE1 + lv + SEPARATE1 + zk + SEPARATE1 + minLv //启用日期+利率+折扣+最低佣金值
        (key, value)
      })
        .groupByKey()
        .collectAsMap()

      //将费率表转换成map结构
      val flbMap = flb.map(row => {
        val fields = row.split(SEPARATE2)
        val zqlb = fields(0)
        //证券类别
        val sh = fields(1)
        //市场
        val lvlb = fields(2)
        //利率类别
        val lv = fields(3) //利率
        val zk = fields(5) //折扣
        val HGdate = fields(6)
        val zch = fields(10) //资产号
        val startDate = fields(13)

        //启用日期
        val key = zqlb + SEPARATE1 + sh + SEPARATE1 + zch + SEPARATE1 + lvlb //证券类别+市场+资产号+利率类别
        val value = startDate + SEPARATE1 + lv + SEPARATE1 + zk + SEPARATE1 + HGdate //启用日期+利率+折扣+回购天数
        (key, value)
      })
        .groupByKey()
        .collectAsMap()

      //交易费用表（佣金的三种模式）
      val csbPath = BasicUtils.getDailyInputFilePath("CSSYSXWFY")
      val jyfy = sc.textFile(csbPath)
      //同一个席位号只能选一个JSF,ZGF
      val jyfyMap = jyfy.map {
        case (row1) => {
          val row = row1.split(",")
          val gddm1 = row(0) //股东代码
          val zqlb = row(4) //证券类别
          val ffylb = row(5) //费用类别 JSF
          val ffyfs = row(6) //费用方式  券商承担 产品承担 计入成本
          val key = gddm1 + SEPARATE1 + zqlb + SEPARATE1 + ffylb
          val value = ffyfs
          (key, value)
        }
      }.collectAsMap()

      /**
        *
        * 读取CSQSFYLV,为了得到券商过户费
        *
        */

      val CSQSFYLV = BasicUtils.getDailyInputFilePath("CSQSFYLV")
      val CSQSFYLVMap = sc.textFile(CSQSFYLV)
      //同一个席位号只能选一个JSF,ZGF
      val qsghf = CSQSFYLVMap.map {
        case (row1) => {
          val row = row1.split(",")
          val zqpz = row(2) //证券品种 GP CDRGP
          val sh = row(3) //市场 SDZ
          val ffylb = row(4) //QSGHF
          val ffyes = row(5)
          val flv = row(8) //利率
          val key = zqpz + SEPARATE1 + sh + SEPARATE1 + ffylb + SEPARATE1 + ffyes
          val value = flv
          (key, value)
        }
      }.collectAsMap()


      /** * 读取基金信息表csjjxx */
      val csjjxxPath = BasicUtils.getDailyInputFilePath("CSJJXX")
      val jjxxb = sc.textFile(csjjxxPath)


      //过滤基金信息表
      val jjxxAarry = jjxxb
        .filter(row => {
          val fields = row.split(SEPARATE2)
          val fzqlx = fields(9)
          val ftzdx = fields(15)
          val fszsh = fields(8)
          if ("ETF".equals(fzqlx) && "S".equals(fszsh) && "ZQ".equals(ftzdx)) true
          else false
        })
        .map(row => {
          val fields = row.split(SEPARATE2)
          fields(1)
        })
        .collect()

      (sc.broadcast(yjbMap), sc.broadcast(flbMap), sc.broadcast(jyfyMap), sc.broadcast(qsghf), sc.broadcast(jjxxAarry))
    }

    val broadcaseFee = loadFeeTables()
    val yjbValues = broadcaseFee._1 //佣金
    val flbValues = broadcaseFee._2 //费率
    val jyfyValues = broadcaseFee._3 //交易费率
    val qsghfValues = broadcaseFee._4
    val jjxxValues = broadcaseFee._5
    val csbValues = csb


    /**
      * 根据套账号，获得资产代码
      */

    def getZCDM() = {
      //交易费用表（佣金的三种模式）
      val listPath = BasicUtils.getDailyInputFilePath("LSETLIST")
      val lSetList = sc.textFile(listPath)
      //同一个席位号只能选一个JSF,ZGF
      val listMap = lSetList.map {
        case (row1) => {
          val row = row1.split(",")
          val fsetid = row(1) // 资产代码
          val fsetcode = row(2) //套账号
          (fsetcode,fsetid)  //根据套账号获取资产代码
        }
      }.collectAsMap()

      (sc.broadcast(listMap), 1)
    }

    val zcdmMap = getZCDM()
    val zcdmValues = zcdmMap._1


    /**
      * 原始数据转换1  带有申请编号
      * key = 本次日期+证券代码+公司代码/交易席位+买卖+股东代码+套账号+证券标志+业务标志+申请编号
      */
    val value = df.rdd.map(row => {
      val bcrq = row.getAs[String]("strDate") //本次日期
      val zqdm = row.getAs[String]("SecurityID") //证券代码
      val gsdm = row.getAs[String]("ReportingPBUID") //公司代码/交易席位
      val gddm = row.getAs[String]("AccountID") //股东代码
      val bs = row.getAs[String]("Side") //买卖
      val sqbh = row.getAs[String]("SQBH") //申请编号
      val tzh = row.getAs[String]("SETCODE") //套账号
      val zqbz = row.getAs[String]("FZQBZ") //证券标志
      val ywbz = row.getAs[String]("FYWBZ") //业务标志
      val key = bcrq + SEPARATE1 + zqdm + SEPARATE1 + gsdm + SEPARATE1 + bs + SEPARATE1 + gddm+ SEPARATE1 + tzh + SEPARATE1 + zqbz + SEPARATE1 + ywbz + SEPARATE1 + sqbh
      (key, row)
    }).groupByKey()

    /**
      * 原始数据转换2 不带申请编号
      * key = 本次日期+证券代码+公司代码/交易席位+买卖+股东代码+套账号+证券标志+业务标志
      */
    val value1 = df.rdd.map(row => {
      val bcrq = row.getAs[String]("strDate") //本次日期
      val zqdm = row.getAs[String]("SecurityID") //证券代码
      val gsdm = row.getAs[String]("ReportingPBUID") //公司代码/交易席位
      val gddm = row.getAs[String]("AccountID") //股东代码
      val bs = row.getAs[String]("Side") //买卖
      val sqbh = row.getAs[String]("SQBH") //申请编号
      val tzh = row.getAs[String]("SETCODE") //套账号
      val zqbz = row.getAs[String]("FZQBZ") //证券标志
      val ywbz = row.getAs[String]("FYWBZ") //业务标志
      val fsetid=zcdmValues.value.getOrElse(tzh, "-1")
      val SH=row.getAs[String]("SH")
      val key = bcrq + SEPARATE1 + zqdm + SEPARATE1 + gsdm + SEPARATE1 + bs + SEPARATE1 +
        gddm + SEPARATE1 + tzh + SEPARATE1 + zqbz + SEPARATE1 + ywbz+SEPARATE1+fsetid+SEPARATE1+SH
      (key, row)
    }).groupByKey()

  //  value1.foreach(println(_))


    def getRate(SH:String,fsetid:String,zqdm: String, gsdm: String, gddm: String, bcrq: String, ywbz1: String, zqbz1: String, zyzch: String, gyzch: String) = {
      //为了获取启动日期小于等于处理日期的参数
      val flbMap = flbValues.value.mapValues(items => {
        val arr = items.toArray.filter(str => (bcrq.compareTo(str.split(SEPARATE1)(0)) >= 0)).sortWith((str1, str2) => (str1.split(SEPARATE1)(0).compareTo(str2.split(SEPARATE1)(0)) > 0))
        //TODO arr's size is 0
        if (arr.size == 0) throw new Exception("未找到适合的公共费率")
        arr(0) //获取处理日期大于启动日期的最大的一个
      })


      val yjMap = yjbValues.value.mapValues(items => {
        val arr = items.toArray.filter(str => (bcrq.compareTo(str.split(SEPARATE1)(0)) >= 0)).sortWith((str1, str2) => (str1.split(SEPARATE1)(0).compareTo(str2.split(SEPARATE1)(0)) > 0))
        //TODO arr's size is 0
        if (arr.size == 0) throw new Exception("未找到合适的佣金费率")
        arr(0)
      })

      var ywbz = ywbz1
      var zqbz = zqbz1
      /** ETF类的要做特殊处理 */
      if (jjxxValues.value.contains(zqdm)) {
        ywbz = "ZQETFJY"
        zqbz = "ZQETFJY"
      }

      if (zqbz.startsWith("HG")) {
        zqbz = "HG" + zqdm
        ywbz = "HG"
      }



      /** 获取费率
        * 将费率类别带进来，根据 证券标志和业务标志  套账号是0还是117来得到费率
        *
        */
      def getCommonFee(fllb: String) = {
        var rateStr = DEFORT_VALUE2
        var maybeRateStr = flbMap.get(ywbz + SEPARATE1 + SH + SEPARATE1 + zyzch + SEPARATE1 + fllb) // 业务标志+S+套账号+JSF
        if (maybeRateStr.isEmpty) {
          maybeRateStr = flbMap.get(zqbz + SEPARATE1 + SH + SEPARATE1 + zyzch + SEPARATE1 + fllb)
          if (maybeRateStr.isEmpty) {
            maybeRateStr = flbMap.get(ywbz + SEPARATE1 + SH + SEPARATE1 + gyzch + SEPARATE1 + fllb)
            if (maybeRateStr.isEmpty) {
              maybeRateStr = flbMap.get(zqbz + SEPARATE1 + SH + SEPARATE1 + gyzch + SEPARATE1 + fllb)
            }
          }
        }
        if (maybeRateStr.isDefined) rateStr = maybeRateStr.get
        val rate = rateStr.split(SEPARATE1)(1) //利率
        val rateZk = rateStr.split(SEPARATE1)(2) //折扣
        val HGdate = rateStr.split(SEPARATE1)(3)
        (rate, rateZk, HGdate)
      }

      /**
        * 获取佣金费率
        * key=业务标志/证券标志+市场+交易席位/股东代码
        * value=启用日期+利率+折扣+最低佣金值
        */
      def getYjFee() = {
        var rateYJStr = DEFORT_VALUE3
        var maybeRateYJStr = DEFORT_VALUE3
        maybeRateYJStr = yjMap.getOrElse(fsetid+SEPARATE1+ywbz + SEPARATE1 + SH + SEPARATE1 + gsdm, "-1")
        if (maybeRateYJStr.equals("-1")) {
          maybeRateYJStr = yjMap.getOrElse(fsetid+SEPARATE1+ywbz + SEPARATE1 + SH + SEPARATE1 + gddm, "-1")
          if (maybeRateYJStr.equals("-1")) {
            maybeRateYJStr = yjMap.getOrElse(fsetid+SEPARATE1+zqbz + SEPARATE1 + SH + SEPARATE1 + gsdm, "-1")
            if (maybeRateYJStr.equals("-1")) {
              maybeRateYJStr = yjMap.getOrElse(fsetid+SEPARATE1+zqbz + SEPARATE1 + SH + SEPARATE1 + gddm, "-1")
            }
          }
        }
        if (maybeRateYJStr != "-1") rateYJStr = maybeRateYJStr
        val rateYJ = rateYJStr.split(SEPARATE1)(1) //利率
        val rateYjzk = rateYJStr.split(SEPARATE1)(2) //折扣
        val minYj = rateYJStr.split(SEPARATE1)(3) //最低佣金
        (rateYJ, rateYjzk, minYj)
      }

      val rateJS = getCommonFee(JSF)

      var rateYH = getCommonFee(YHS)

      var rateZG = getCommonFee(ZGF)

      var rateGH = getCommonFee(GHF)

      var rateFXJ = getCommonFee(FXJ)

      var rateSXF = getCommonFee(SXF)


      val yjFee = getYjFee()

      (rateJS._1, rateJS._2, rateYH._1, rateYH._2, rateZG._1, rateZG._2, rateGH._1, rateGH._2, rateFXJ._1, rateFXJ._2, rateSXF._1, rateSXF._2, rateSXF._3, yjFee._1, yjFee._2, yjFee._3)
    }

    /**
      * 根据套账号获取公共参数
      *
      * @param tzh 套账号
      **/
    def getGgcs(tzh: String) = {
      //获取是否的参数
      val cs1 = csbValues.value.getOrElse(tzh + CS1_KEY, "-1") //是否开启佣金包含经手费，证管费
      var cs2 = csbValues.value.getOrElse(tzh + CON18_KEY, "-1") //深圳佣金计算保留位数
      val cs3 = csbValues.value.getOrElse(tzh + CON23_KEY, "-1") //深圳佣金计算费用保留位数
      val cs4 = csbValues.value.getOrElse(tzh + CS4_KEY, "-1") //是否开启计算佣金减去风险金
      val cs5 = csbValues.value.getOrElse(tzh + CS6_KEY, "-1") //是否开启计算佣金减去结算费
      val cs6 = csbValues.value.getOrElse(CON24_KEY, "-1") //深交所证管费和经手费分别计算


      (cs1, cs2, cs3, cs4, cs5, cs6)
    }

    /**
      * 根据套账号获取计算参数
      *
      * @param tzh 套账号
      * @return
      */
    def getJsgz(tzh: String) = {
      val cs6 = csbValues.value.getOrElse(tzh + CON8_KEY, "-1") //是否开启实际收付金额包含佣金

    }

    //第一种   不带申请编号   每一笔交易单独计算，最后相加
    val fee1 = value1.map {
      case (key, values) =>
        val fields = key.split(SEPARATE1)
        val bs = fields(3) //买卖方向
        val gsdm = fields(2) //交易席位
        val bcrq = fields(0) //本次日期
        val zqdm = fields(1) //证券代码
        val gddm = fields(4) //股东代码
        val tzh = fields(5) //套账号
        val zqbz = fields(6) //证券标志
        val ywbz = fields(7) //业务标志
        val fsetid=fields(8) //资产代码
        val SH=fields(9)



        val getRateResult = getRate(SH,fsetid,zqdm, gsdm, gddm, bcrq, ywbz, zqbz, tzh, GYZCH)
        val rateJS: String = getRateResult._1
        val rateJszk: String = getRateResult._2
        val rateYH: String = getRateResult._3
        val rateYhzk: String = getRateResult._4
        val rateZG: String = getRateResult._5
        val rateZgzk: String = getRateResult._6
        val rateGH: String = getRateResult._7
        val rateGhzk: String = getRateResult._8
        val rateFXJ: String = getRateResult._9
        val rateFxjzk: String = getRateResult._10
        val rateSXF: String = getRateResult._11
        val rateSXFzk: String = getRateResult._12
        val HGDate: String = getRateResult._13
        val rateYJ: String = getRateResult._14
        val rateYjzk: String = getRateResult._15
        val minYj: String = getRateResult._16

        val otherFee = BigDecimal(0)
        var sumCjje = BigDecimal(0) //总金额
      var sumCjsl = BigDecimal(0) //总数量
      var sumYj = BigDecimal(0) //总的佣金
      var sumJsf = BigDecimal(0) //总的经手费
      var sumYhs = BigDecimal(0) //总的印花税
      var sumZgf = BigDecimal(0) //总的征管费
      var sumGhf = BigDecimal(0) //总的过户费
      var sumFxj = BigDecimal(0) //总的风险金
      var sumSXF = BigDecimal(0) //手续费
      var sumSQGHF = BigDecimal(0) //券商过户费
      var sumFqtf=BigDecimal(0)  //其他费用
      var sumHgsy=BigDecimal(0) //回购收益


        var yhs = BigDecimal(0)
        var jsf = BigDecimal(0)
        var zgf = BigDecimal(0)
        var ghf = BigDecimal(0)
        var fx = BigDecimal(0)
        var Yj = BigDecimal(0)
        var sxf = BigDecimal(0)
        var sqghf = BigDecimal(0)
        var hgsy=BigDecimal(0)

        val csResults = getGgcs(tzh)
        val cs1 = csResults._1
        var cs2 = csResults._2 //深圳佣金计算费用保留位数
        val cs3 = csResults._3
        val cs4 = csResults._4
        val cs5 = csResults._5
        val cs6 = csResults._6 //深交所证管费和经手费分别计算


        for (row <- values) {
          val cjje1 = BigDecimal(row.getAs[String]("LastPx"))
          val cjsl = BigDecimal(row.getAs[String]("LastQty"))
          var cjje = cjje1.*(cjsl)

          //经手费的计算
          if (cs6.equals("-1") || cs6.equals("0")) { // 不启用 经手费 =成交金额*(经手费率*折扣率+征管费*折扣率)
            jsf = cjje.*(BigDecimal(rateJS).*(BigDecimal(rateJszk)) + BigDecimal(rateZG).*(BigDecimal(rateZgzk))).setScale(0, RoundingMode.HALF_UP)
          } else {
            jsf = cjje.*(BigDecimal(rateJS)).*(BigDecimal(rateJszk)).setScale(0, RoundingMode.HALF_UP)
          }

          //计算回购收益
          //回购数量＝回报成交数量；回购金额＝成交数量×100
          //经手费：（到期时为0）
          //如果回购金额<=1000000，则经手费＝0.1；
          // 如果回购金额> 1000000，则经手费＝1。
          if(zqbz.equals("HG")){
            hgsy = ((cjje1.*(BigDecimal(HGDate)))./(365)).setScale(2, RoundingMode.HALF_UP).*(cjsl).setScale(2, RoundingMode.HALF_UP)
            cjje=cjsl.*(100)
            if(cjje<=1000000){
              jsf=BigDecimal(0.1)
            }else if(cjje>1000000){
              jsf=BigDecimal(1)
            }

          }else if(zqbz.equals("QZ")){
            sxf = (cjje.*(BigDecimal(rateSXF)).*(BigDecimal(rateSXFzk))).setScale(2, RoundingMode.HALF_UP)
          }

          if (cs3.equals("-1") || cs3.equals("0")) {
            // 买不计算印花税
            if (SALE.equals(bs)) {
              //印花税的计算
              yhs = cjje.*(BigDecimal(rateYH)).*(BigDecimal(rateYhzk)).setScale(0, RoundingMode.HALF_UP)
            }
            //征管费的计算
            zgf = cjje.*(BigDecimal(rateZG)).*(BigDecimal(rateZgzk)).setScale(0, RoundingMode.HALF_UP)




            //风险金的计算
            fx = cjje.*(BigDecimal(rateFXJ)).*(BigDecimal(rateFxjzk)).setScale(0, RoundingMode.HALF_UP)

            //过户费的计算
            ghf = cjje.*(BigDecimal(rateGH)).*(BigDecimal(rateGhzk)).setScale(0, RoundingMode.HALF_UP)
          } else {
            // 买不计算印花税
            if (SALE.equals(bs)) {
              //印花税的计算
              yhs = cjje.*(BigDecimal(rateYH)).*(BigDecimal(rateYhzk)).setScale(cs3.toInt, RoundingMode.HALF_UP)
            }
            //征管费的计算
            zgf = cjje.*(BigDecimal(rateZG)).*(BigDecimal(rateZgzk)).setScale(cs3.toInt, RoundingMode.HALF_UP)

            //风险金的计算
            fx = cjje.*(BigDecimal(rateFXJ)).*(BigDecimal(rateFxjzk)).setScale(cs3.toInt, RoundingMode.HALF_UP)

            //过户费的计算
            ghf = cjje.*(BigDecimal(rateGH)).*(BigDecimal(rateGhzk)).setScale(cs3.toInt, RoundingMode.HALF_UP)
          }

          //计算券商过户费
          val qsghf1 = qsghfValues.value.getOrElse(zqbz + SEPARATE1 + SH + SEPARATE1 + "QSGHF" + SEPARATE1 + "0", "-1")
          val qsghf2 = qsghfValues.value.getOrElse(ywbz + SEPARATE1 + SH + SEPARATE1 + "QSGHF" + SEPARATE1 + "0", "-1")

          if (cs2.equals("-1") || cs2.equals("0")) {
            Yj = cjje.*(BigDecimal(rateYJ)).*(BigDecimal(rateYjzk)).setScale(0, RoundingMode.DOWN)
          } else {
            Yj = cjje.*(BigDecimal(rateYJ)).*(BigDecimal(rateYjzk)).setScale(cs2.toInt, RoundingMode.HALF_UP)
          }


          //当为GP时，佣金-券商过户费-过户费
          if (qsghf1 != "-1") {
            sqghf = cjsl.*(BigDecimal(qsghf1)).setScale(2, RoundingMode.HALF_UP)
            Yj = Yj - sqghf + ghf
          } else if (qsghf2 != "-1") {
            sqghf = cjsl.*(BigDecimal(qsghf2)).setScale(2, RoundingMode.HALF_UP)
            Yj = Yj - sqghf + ghf
          }


          if (NO.equals(cs1)) { //经手费,证管费
            Yj = Yj.-(jsf).-(zgf)
          } else if (YES.equals(cs1)) {
            var JSF = jyfyValues.value.getOrElse(gddm + SEPARATE1 + zqbz + SEPARATE1 + "JSF", "-1") //如果这个key的话，取得这个值，没有取-1
            var ZGF = jyfyValues.value.getOrElse(gddm + SEPARATE1 + zqbz + SEPARATE1 + "ZGF", "-1")
            var JSF1 = jyfyValues.value.getOrElse(gsdm + SEPARATE1 + zqbz + SEPARATE1 + "JSF", "-1")
            var ZGF1 = jyfyValues.value.getOrElse(gsdm + SEPARATE1 + zqbz + SEPARATE1 + "ZGF", "-1")
            var JSF2 = jyfyValues.value.getOrElse(gsdm + SEPARATE1 + ywbz + SEPARATE1 + "JSF", "-1")
            var ZGF2 = jyfyValues.value.getOrElse(gsdm + SEPARATE1 + ywbz + SEPARATE1 + "ZGF", "-1")

            if (JSF == 0 || JSF1 == 0 || JSF == 0) { //有经手费，有证管费

              if (ZGF == 0 || ZGF1 == 0 || ZGF2 == 0) {
                //有经手费，有证管费

                //佣金-征管费-经手费
                Yj = Yj - jsf - zgf
              } else {
                //有经手费，没证管费
                //佣金-经手费
                Yj = Yj - jsf
              }
            } else if (ZGF == 0 || ZGF1 == 0 || ZGF2 == 0) { //没有经手费 ，有证管费
              Yj = Yj - zgf
            }
            Yj
          }
          if (YES.equals(cs4)) {
            Yj = Yj.-(fx)
          }
          if(YES.equals(cs6)){
            Yj=Yj.-(zgf).-(jsf)

          }

          if (YES.equals(cs5)) {
            Yj = Yj.-(otherFee)
          }
          if (Yj < BigDecimal(minYj)) {
            Yj = BigDecimal(minYj)
          }


          sumCjje = sumCjje.+(cjje)
          sumCjsl = sumCjsl.+(cjsl)
          //          sumYj = sumYj.+(yj)
          sumJsf = sumJsf.+(jsf)
          sumYhs = sumYhs.+(yhs)
          sumZgf = sumZgf.+(zgf)
          sumGhf = sumGhf.+(ghf)
          sumFxj = sumFxj.+(fx)
          sumSXF = sumSXF.+(sxf)
          sumYj = sumYj.+(Yj)
          sumSQGHF = sumSQGHF.+(sqghf)
          sumHgsy=sumHgsy.+(hgsy)
        }

        // sumYj = sumCjje.*(BigDecimal(rateYJ)).*(BigDecimal(rateYjzk)).setScale(2, RoundingMode.HALF_UP)

        (key, SJSObj("1", sumCjje, sumCjsl, sumYj, sumJsf, sumYhs, sumZgf,
          sumGhf, sumFxj, sumSXF, sumSQGHF,sumHgsy))
    }


    //最终结果
    val result = fee1.map {
      case (key, fee1) =>
        val fields = key.split(SEPARATE1)
        val bs = fields(3) //买卖方向
        val gsdm = fields(2) //交易席位
        val bcrq = fields(0) //本次日期
        val zqdm = fields(1) //证券代码
        val gddm = fields(4)
        val tzh = fields(5) //套账号
        val zqbz = fields(6)
        val ywbz = fields(7)
        val zcdm = fields(8)
        val SH=fields(9)


        val totalCjje = fee1.sumCjje
        val totalCjsl = fee1.sumCjsl
        //  val fgzlx = fee1.sumGzlx
        //  val fhggain = fee1.sumHgsy
        var realYj = BigDecimal(0)
        var realJsf = BigDecimal(0)
        var realYhs = BigDecimal(0)
        var realZgf = BigDecimal(0)
        var realGhf = BigDecimal(0)
        var realFxj = BigDecimal(0)
        var realSxf = BigDecimal(0)
        var realQsghf = BigDecimal(0)
        var realHgsy = BigDecimal(0)

        val jsResult = getJsgz(tzh)
        val con8 = getJsgz(tzh)
        //判断取值逻辑
          realJsf = fee1.sumJsf
          realZgf = fee1.sumZgf
          realGhf = fee1.sumGhf
          realYhs = fee1.sumYhs
          realYj = fee1.sumYj
          realFxj = fee1.sumFxj
          realSxf = fee1.sumSXF
          realQsghf = fee1.sumQSGHF
          realHgsy=fee1.sumhGSY

        var fsfje=BigDecimal(0)

        if(SALE.equals(bs)){
          if(zqbz.equals("QZ")){
            fsfje= totalCjje.-(realYhs).+(realSxf).-(realZgf).-(realGhf).-(realJsf)
          }else{
            fsfje = totalCjje.-(realJsf).-(realZgf).-(realGhf).-(realYhs)
          }
        }else{
          if(zqbz.equals("QZ")){
            fsfje= totalCjje.+(realYhs).+(realSxf).+(realZgf).+(realGhf).+(realJsf)
          }else{
            fsfje = totalCjje.+(realJsf).+(realZgf).+(realGhf)
          }
        }


        if (YES.equals(con8)) {
          if(SALE.equals(bs)){
            fsfje -= realYj
          }else{
            fsfje += realYj
          }
        }


        ExecutionObj(zcdm, bcrq,
          bcrq, zqdm, SH, gsdm, bs,
          totalCjje.formatted("%.2f"),
          totalCjsl.formatted("%.2f"),
          realYj.formatted("%.2f"),
          realJsf.formatted("%.2f"),
          realYhs.formatted("%.2f"),
          realZgf.formatted("%.2f"),
          realGhf.formatted("%.2f"),
          realFxj.formatted("%.2f"),
          "0",
          realHgsy.formatted("%.2f"),
          fsfje.formatted("%.2f"),
          zqbz, ywbz,
          "N",  realSxf.formatted("%.2f"), zqdm, "PT", "1", "", "", "0", "", realQsghf.formatted("%.2f"),
          gddm, "", "", "", "", "", "", "","", "RMB", "", "", "", "", ""
        )
    }
    //将结果输出
    import spark.implicits._
    BasicUtils.outputMySql(result.toDF(), "SZSTOCKK")
    //Util.outputHdfs(result.toDF(),"/yss/guzhi/hzjkqs/20181106/Execution/")
    result.toDF.show(100)
  }
}
