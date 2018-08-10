package com.yss.scala.guzhi

case class ExecutionAggr(
 Fdate:String ,//日期
 FinDate:String, //读入日期  读取日期
 FZqdm:String, //证券代码    SecurityID
 FSzsh:String, //市场         S
 Fjyxwh:String, // 交易席位号   ReportingPBUID
 Fbje:String, //买金额   side=1 SUM(LastPx * LastQty)
 Fsje: String,//卖金额  side=2 SUM(LastPx * LastQty)
 FBsl:String, //买数量   side=1  SUM(LastQty)
 FSsl:String,//卖数量    side=2 SUM(LastQty)
 Fbyj:String, //买佣金  side=1  SUM(fbje*佣金费率 - 证管费 -过户费-印花费)
 Fsyj:String,//卖佣金  side=2 SUM(fsje*佣金费率-证管费-过户费-印花费)
 FBjsf:String,//买经手费   side=1  SUM(fbje* 经手费率)
 FSjsf:String,//卖经手费  side=2  SUM(fsje* 经手费率)
 Fbyhs:String, //买印花税  side=1 SUM(fbje * 印花费率)
 Fsyhs:String, //卖印花税  side=2 SUM(fbje * 印花费率)
 FBzgf:String, //买证管费 side=1  SUM(fbje * 证管费率)
 FSzgf:String,//卖证管费 side=2  SUM(fbje*征管费率)
 FBghf:String,//买过户费 side=1 SUM(fbje * 过户费率)
 FSghf:String,//卖过户费率 side=2 SUM(fbje * 过户费率)
 FBgzlx:String ,//买国债利息  side=1  " "
 FSgzlx:String , //卖国债利息  side=2  " "
 FBfxj:String, //买风险金  side=1   SUM(fbje * 风险金费率)
 FSfxj:String, //卖风险金 side=2  SUM(fbje * 风险金费率)
 Fbsfje :String,// 买实付金额 Fbje+FBjsf+FBzgf+FBghf
 Fsssje:String,//卖实收金额 Fsje-FSjsf-FSzgf-FSghf-Fsyhs
 FZqbz:String,//证券标志  ApplID
 Fywbz:String,//业务标志  PT
 FQsbz:String,//清算标志 N
 FBQTF:String,//买其他费用   成交数量 * 成交价格 * 手续费利率
 FSQTF:String,//卖其他费用   成交数量 * 成交价格 * 手续费利率
 ZQDM:String,//证券代码   SecurityID
 FJYFS:String,//交易方式 PT
 Fsh:Int,//审核  1 :审核，0，未审核
 FZZR:String ,//制作人 " "
 FCHK:String,//审核人 " "
 fzlh:String,//指令号 0
 ftzbz:String ,//投资标志 " "
 FBQsghf:String,//买券商过户费   面值 * 费率
 FsQsghf:String,//卖券商过户费  面值* 费率
 fgddm:String //文件字段    AccountID
                        )







