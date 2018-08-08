package com.yss.scala.guzhi

case class ExecutionAggr(
 Fdate:String ,//日期
 FinDate:String, //读入日期  读取日期
 FZqdm:String, //证券代码    SecurityID
 FSzsh:String, //市场         S
 Fjyxwh:String, // 交易席位号   ReportingPBUID
 Fbje:BigDecimal, //买金额   side=1 SUM(LastPx * LastQty)
 Fsje: BigDecimal,//卖金额  side=2 SUM(LastPx * LastQty)
 FBsl:BigDecimal, //买数量   side=1  SUM(LastQty)
 FSsl:BigDecimal,//卖数量    side=2 SUM(LastQty)
 Fbyj:BigDecimal, //买佣金  side=1  SUM(fbje*佣金费率 - 证管费 -过户费-印花费)
 Fsyj:BigDecimal,//卖佣金  side=2 SUM(fsje*佣金费率-证管费-过户费-印花费)
 FBjsf:BigDecimal,//买经手费   side=1  SUM(fbje* 经手费率)
 FSjsf:BigDecimal,//卖经手费  side=2  SUM(fsje* 经手费率)
 Fbyhs:BigDecimal, //买印花税  side=1 SUM(fbje * 印花费率)
 Fsyhs:BigDecimal, //卖印花税  side=2 SUM(fbje * 印花费率)
 FBzgf:BigDecimal, //买证管费 side=1  SUM(fbje * 证管费率)
 FSzgf:BigDecimal,//卖证管费 side=2  SUM(fbje*征管费率)
 FBghf:BigDecimal,//买过户费 side=1 SUM(fbje * 过户费率)
 FSghf:BigDecimal,//卖过户费率 side=2 SUM(fbje * 过户费率)
 FBgzlx:BigDecimal ,//买国债利息  side=1  " "
 FSgzlx:BigDecimal , //卖国债利息  side=2  " "
 FBfxj:BigDecimal, //买风险金  side=1   SUM(fbje * 风险金费率)
 FSfxj:BigDecimal, //卖风险金 side=2  SUM(fbje * 风险金费率)
 Fbsfje :BigDecimal,// 买实付金额 Fbje+FBjsf+FBzgf+FBghf
 Fsssje:BigDecimal,//卖实收金额 Fsje-FSjsf-FSzgf-FSghf-Fsyhs
 FZqbz:String,//证券标志  ApplID
 Fywbz:String,//业务标志  PT
 FQsbz:String,//清算标志 N
 FBQTF:BigDecimal,//买其他费用   成交数量 * 成交价格 * 手续费利率
 FSQTF:BigDecimal,//卖其他费用   成交数量 * 成交价格 * 手续费利率
 ZQDM:String,//证券代码   SecurityID
 FJYFS:String,//交易方式 PT
 Fsh:Int,//审核  1 :审核，0，未审核
 FZZR:String ,//制作人 " "
 FCHK:String,//审核人 " "
 fzlh:String,//指令号 0
 ftzbz:String ,//投资标志 " "
 FBQsghf:BigDecimal,//买券商过户费   面值 * 费率
 FsQsghf:BigDecimal,//卖券商过户费  面值* 费率
 fgddm:String //文件字段    AccountID
                        )







