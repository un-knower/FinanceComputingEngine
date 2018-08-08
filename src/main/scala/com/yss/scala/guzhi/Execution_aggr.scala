package com.yss.scala.guzhi

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
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
    val exe = sc.textFile("C:\\Users\\hgd\\Desktop\\估值资料\\execution_aggr_N000032F0001_1_20160825.tsv")/*execution_aggr_N000032F0001_1_20160825*/


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
    var Fbje = BigDecimal(0.0) //买金额
    var Fsje = BigDecimal(0.0) //卖金额
    var FBsl = BigDecimal(0.0) //买数量
    var FSsl = BigDecimal(0.0) //卖数量
    var Fbyj = BigDecimal(0.0) //买佣金
    var Fsyj = BigDecimal(0.0) //卖佣金
    var FBjsf = BigDecimal(0.0) //买经手费
    var FSjsf = BigDecimal(0.0) //卖经手费
    var Fbyhs = BigDecimal(0.0) //买印花税
    var Fsyhs = BigDecimal(0.0) //卖印花税
    var FBzgf = BigDecimal(0.0) //买征管费
    var FSzgf = BigDecimal(0.0) //卖征管费
    var FBghf = BigDecimal(0.0) //买过户费
    var FSghf = BigDecimal(0.0) //卖过户费
    var FBfxj = BigDecimal(0.0) //买风险金
    var FSfxj = BigDecimal(0.0) //卖风险金
    var Fbsfje=BigDecimal(0.0) //买实付金额
    var Fsssje=BigDecimal(0.0) //卖实收金额



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
    val FBQTF=BigDecimal(0)  //买其他费用
    val FSQTF=BigDecimal(0)//卖其他费用
    val FJYFS="PT"
    val Fsh=1
    val FZZR=" "
    val FCHK=" "
    val fzlh="0"
    val ftzbz=" "
    val FBQsghf=BigDecimal(0)
    val FsQsghf=BigDecimal(0)

    //4.进行计算
     val finallData= groupKey.flatMap{
          case(key,iterable)=>{
             var execution=new mutable.ArrayBuffer[ExecutionAggr]()
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
              Fbyhs += Fbje.*(PrintingRate)
              FBzgf += Fbje.*(CollectionRate)
              FBghf += Fbje.*(TransferRate)
              FBfxj += Fbje.*(RiskRate)
              Fbyj += Fbje.*(commisionRate) - FBzgf - FBghf - Fbyhs //买佣金
              Fbsfje += Fbje + FBjsf + FBzgf + FBghf
              FBjsf += Fbje.*(HandingFeeRate)
              val executionAggr=new ExecutionAggr(TransactTime,TransactTime,SecurityID,Market,ReportingPBUID,Fbje.setScale(2),BigDecimal(0),FBsl
                ,BigDecimal(0),Fbyj,BigDecimal(0),FBjsf,BigDecimal(0),Fbyhs,BigDecimal(0),FBzgf,BigDecimal(0)
                ,FBghf,BigDecimal(0),BigDecimal(0),BigDecimal(0),FBfxj,BigDecimal(0),Fbsfje,BigDecimal(0),FZqbz,Fywbz,FQsbz,FBQTF,FSQTF,SecurityID,FJYFS,Fsh,FZZR,FCHK,fzlh,ftzbz,FBQsghf,FsQsghf,AccountID)

              execution += executionAggr


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
              val executionAggr=new ExecutionAggr(TransactTime,TransactTime,SecurityID,Market,ReportingPBUID,BigDecimal(0),Fsje.setScale(2,BigDecimal.RoundingMode.CEILING),BigDecimal(0)
                ,FSsl,BigDecimal(0),Fsyj,BigDecimal(0),FSjsf,BigDecimal(0),Fsyhs,BigDecimal(0),FSzgf
                ,BigDecimal(0),FSghf,BigDecimal(0),BigDecimal(0),FBfxj,FSfxj,BigDecimal(0),Fsssje,FZqbz,Fywbz,FQsbz,FBQTF,FSQTF,SecurityID,FJYFS,Fsh,FZZR,FCHK,fzlh,ftzbz,FBQsghf,FsQsghf,AccountID)
              execution += executionAggr
            }
            execution
          }
        }
  import sparkSession.implicits._
       finallData.toDF().write.format("jdbc")
    .option("url","jdbc:mysql://192.168.102.119:3306/test")
    .option("user","root")
    .option("password","root1234")
    .option("dbtable","SJSV5")
    .mode(SaveMode.Append)
    .save()
   // finallData.toDF().show()
  }
}
