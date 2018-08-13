package com.yss.scala.guzhi

import java.sql.DriverManager
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
/**
  * @author ChenYao
  * date 2018/8/8
  *
  *数据源:execution_aggr_N000032F0001_1_20160825.tsv
  *数据处理:{日期 +SecurityID +市场(S)+交易席位(ReportingPBUID)+SIDE(买卖)+股东代码(AccountID)} 相同的进行买入和卖出的计算
  *         输出到mysql数据库JJCWGZ 的表 SJSV5中
  */
object Execution_aggr {
  def main(args: Array[String]): Unit = {
    //1.将tsv文件读进来
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SJSV5")
    val sc = new SparkContext(sparkConf)
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val exe = sc.textFile("C:\\Users\\hgd\\Desktop\\估值资料\\execution_aggr_F000995F0401_1_20180808(1).tsv")/*execution_aggr_N000032F0001_1_20160825*/
   /*hdfs://nscluster/yss/guzhi/execution_aggr_N000032F0001_1_20160825.tsv*/
    /*C:\Users\hgd\Desktop\估值资料\exe.tsv*/

    //2.进行map,将数据按 | 分割
    val map1 = exe.map {
      case (text) => {
        val word = text.split("\t")
        val ReportingPBUID = word(3) //回报交易单元
        val SecurityID = word(5) //证券代码
        val TransactTime = word(9) //回报时间
        val Side = word(20) //买卖方向
        val AccountID = word(21) //证券账户
        val Market="S"
        val key=TransactTime+"_"+SecurityID+"_"+Market+"_"+ReportingPBUID+"_"+Side+"_"+AccountID
        (key,text)  //key  日期 +SecurityID +市场(S)+交易席位(ReportingPBUID)+SIDE(买卖)+股东代码(AccountID)
      }
    }

    //3.进行分组
    val groupKey=map1.groupByKey()
    //4.进行计算
     val finallData= groupKey.flatMap{
          case(key,iterable)=>{
            var Fbje = BigDecimal(0.00)//买金额
            var Fsje = BigDecimal(0.00) //卖金额
            var FBsl = BigDecimal(0.00) //买数量
            var FSsl = BigDecimal(0.00) //卖数量
            var Fbyj = BigDecimal(0.00) //买佣金
            var Fsyj = BigDecimal(0.00) //卖佣金
            var FBjsf = BigDecimal(0.00) //买经手费
            var FSjsf = BigDecimal(0.00) //卖经手费
            var Fbyhs = BigDecimal(0.00) //买印花税
            var Fsyhs = BigDecimal(0.00) //卖印花税
            var FBzgf = BigDecimal(0.00) //买征管费
            var FSzgf = BigDecimal(0.00) //卖征管费
            var FBghf = BigDecimal(0.00) //买过户费
            var FSghf = BigDecimal(0.00) //卖过户费
            var FBfxj = BigDecimal(0.00) //买风险金
            var FSfxj = BigDecimal(0.00) //卖风险金
            var Fbsfje=BigDecimal(0.00) //买实付金额
            var Fsssje=BigDecimal(0.00) //卖实收金额



            //费率 1.佣金费率，经手费率，印花费率，过户费率，证管费率，风险金费率
            val commisionRate =BigDecimal( 0.0025)//佣金费率
            val HandingFeeRate = BigDecimal(0.0001475) //经手费率
            val PrintingRate = BigDecimal(0.001) //印花费率
            val TransferRate = BigDecimal(0.00004)//过户费率
            val CollectionRate = BigDecimal(0.00004) //征管费率
            val RiskRate = BigDecimal(0.00004) //风险金费率



            val FZqbz="GP" //证券标志
            val Fywbz="PT" //业务标志
            val FQsbz="N"  //清算标志
            val FBQTF=BigDecimal(0.00)  //买其他费用
            val FSQTF=BigDecimal(0.00)//卖其他费用
            val FJYFS="PT"
            val Fsh=1
            val FZZR=" "
            val FCHK=" "
            val fzlh="0"
            val ftzbz=" "
            val FBQsghf=BigDecimal(0.00)
            val FsQsghf=BigDecimal(0.00)
             var execution=new /*mutable.ArrayBuffer[ExecutionAggr]()*/ ListBuffer[ExecutionAggr]()
                 //1）将key进行切分
                    val keys=key.split("_")
                    val TransactTime = keys(0)//回报时间
                    val SecurityID=keys(1)
                    val Market=keys(2)
                    val ReportingPBUID = keys(3) //回报交易单元
                    val Side = keys(4) //买卖方向
                    val AccountID = keys(5) //证券账户
                 //2) 如果Side==1,将所有的进行计算，将结果输出到ExecutionAggr中

            if(Side=="1") {
              for (func <- iterable) { //将所有写到ExecutionAggr,最后将类放到可变数组中
                val text = func.split("\t")
                val LastPx = BigDecimal(text(16)) //成交价
                val LastQty = BigDecimal(text(17)) //成交数量
                val LeavesQty = text(18) //订单剩余数量
                //进行计算
                Fbje += LastPx.*(LastQty)
                FBsl += LastQty
              }
              Fbyhs =BigDecimal(0.0)
              FBzgf += Fbje.*(CollectionRate)
              FBghf += Fbje.*(TransferRate)
              FBfxj += Fbje.*(RiskRate)
              Fbyj += Fbje.*(commisionRate) - FBzgf - FBghf - Fbyhs //买佣金
              Fbsfje += Fbje + FBjsf + FBzgf + FBghf
              FBjsf += Fbje.*(HandingFeeRate)

              val zero=BigDecimal(0).formatted("%.2f")
              val executionAggr=new ExecutionAggr(TransactTime,TransactTime,SecurityID,Market,ReportingPBUID,Fbje.formatted("%.2f"),zero,FBsl.formatted("%.2f")
                ,zero,Fbyj.formatted("%.2f"),zero,FBjsf.formatted("%.2f"),zero,Fbyhs.formatted("%.2f"),zero,FBzgf.formatted("%.2f"),zero
                ,FBghf.formatted("%.2f"),zero,zero,zero,FBfxj.formatted("%.2f"),zero,Fbsfje.formatted("%.2f"),zero,FZqbz,Fywbz,FQsbz,FBQTF.formatted("%.2f"),FSQTF.formatted("%.2f"),SecurityID,FJYFS,Fsh,FZZR,FCHK,fzlh,ftzbz,FBQsghf.formatted("%.2f"),FsQsghf.formatted("%.2f"),AccountID)

              execution.append(executionAggr)


              //3) 如果 Side==2,将所有的进行计算，输出到ExecutionAggr中
            }else if(Side=="2"){

              for (func <- iterable) { //将所有写到ExecutionAggr,最后将类放到可变数组中
                val text = func.split("\t")
                val LastPx = BigDecimal(text(16)) //成交价
                val LastQty = BigDecimal(text(17)) //成交数量
                val LeavesQty = text(18) //订单剩余数量
                //进行计算
                Fsje += LastPx.*(LastQty)  //卖金额
                FSsl += LastQty  //卖数量
              }
              Fsyhs += Fsje.*(PrintingRate)
              FSjsf += Fsje.*(HandingFeeRate)
              FSzgf += Fsje.*(CollectionRate)
              FSghf += Fsje.*(TransferRate)
              FSfxj += Fsje.*(RiskRate)
              Fsyj += Fsje.*(commisionRate) - FSzgf - FSghf - Fsyhs //卖佣金
              Fsssje += Fsje - FSjsf - FSzgf - FSghf-Fsyhs
              val zero=BigDecimal(0).formatted("%.2f")
              val executionAggr=new ExecutionAggr(TransactTime,TransactTime,SecurityID,Market,ReportingPBUID,0.toString,Fsje.formatted("%.2f") ,zero
                ,FSsl.formatted("%.2f"),zero,Fsyj.formatted("%.2f"),zero,FSjsf.formatted("%.2f"),zero,Fsyhs.formatted("%.2f"),zero,FSzgf.formatted("%.2f")
                ,zero,FSghf.formatted("%.2f"),zero,zero,FBfxj.formatted("%.2f"),FSfxj.formatted("%.2f"),zero,Fsssje.formatted("%.2f"),FZqbz,Fywbz,FQsbz,FBQTF.formatted("%.2f"),FSQTF.formatted("%.2f"),SecurityID,FJYFS,Fsh,FZZR,FCHK,fzlh,ftzbz,FBQsghf.formatted("%.2f"),FsQsghf.formatted("%.2f"),AccountID)
              execution.append(executionAggr)
            }
            execution
          }
        }

   /*val dbc = "jdbc:mysql://192.168.102.119:3306/test?user=root&password=root1234" /*JJCWGZ*/
    classOf[com.mysql.jdbc.Driver]
    val conn = DriverManager.getConnection(dbc)


    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://192.168.102.119:3306/test"
    val username = "root"
    val password = "root1234"

    // do database insert
    try {
      Class.forName(driver)
      val connection = DriverManager.getConnection(url, username, password)


      val prep = conn.prepareStatement("create table SJSV5(" +
        "  `FDATE`   VARCHAR(30) not null,  `FINDATE` VARCHAR(30)  not null," +
        "  `FZQDM`   VARCHAR(10) not null,  `FSZSH`   VARCHAR(1) not null," +
        "  `FJYXWH`  VARCHAR(10) not null,  `FBJE`    decimal(18,2) not null," +
        "  `FSJE`   decimal(18,2) not null,  `FBSL`   decimal(18,2) not null," +
        "  `FSSL`   decimal(18,2) not null,  `FBYJ`   decimal(18,2) not null," +
        "  `FSYJ`   decimal(18,2) not null,  `FBJSF`  decimal(18,2) not null," +
        "  `FSJSF`  decimal(18,2) not null,  `FBYHS`  decimal(18,2) not null," +
        "  `FSYHS`  decimal(18,2) not null, `FBZGF`  decimal(18,2) not null," +
        "  `FSZGF`   decimal(18,2) not null,  `FBGHF`  decimal(18,2) not null, " +
        " `FSGHF`  decimal(18,2) not null, `FBGZLX` decimal(18,2) not null," +
        "  `FSGZLX` decimal(18,2) not null,  `FBFXJ`  decimal(18,2) not null," +
        " `FSFXJ`  decimal(18,2) not null,  `FBSFJE` decimal(18,2) not null," +
        "  `FSSSJE` decimal(18,2) not null,  `FZQBZ`   VARCHAR(20) not null," +
        "  `FYWBZ`   VARCHAR(20) not null,  `FQSBZ`   VARCHAR(1) not null," +
        "  `FBQTF`  decimal(18,2) not null,  `FSQTF`  decimal(18,2) not null," +
        "  `ZQDM`    VARCHAR(10) not null,  `FJYFS`   VARCHAR(10) default 'PT' not null," +
        "  `FSH`     INTEGER default 1,  `FZZR`    VARCHAR(20) default ' ' not null," +
        "  `FCHK`    VARCHAR(20) default ' ',  `FZLH`    VARCHAR(30) default '0' not null," +
        "  `FTZBZ`   VARCHAR(1) default ' ' not null, `FBQSGHF` decimal(18,2) default 0," +
        "  `FSQSGHF` decimal(18,2) default 0,  `FGDDM`   VARCHAR(18) default ' ' not null) ")
    }
    finally {
      conn.close
    }*/


/*import sparkSession.implicits._
       finallData.toDF().write.format("jdbc")
    .option("url","jdbc:mysql://192.168.102.119:3306/JJCWGZ")
    .option("user","root")
    .option("password","root1234")
    .option("dbtable","SJSV5")
    .mode(SaveMode.Append)
    .save()*/
import sparkSession.implicits._
  finallData.toDF().show()
  }
}
