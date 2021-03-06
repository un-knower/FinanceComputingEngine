package com.yss.scala.core

object ExecutionContaints {
  //定义分隔符
  val SEPARATE1 = "@"
  val SEPARATE2 = ","
  /** 分隔符 - */
  val SEPARATE3 = "-"
  /** 资产类型 */
  val ZCLB = "GP"

  /** 公用资产号 */
  val GYZCH = "0"
  /** 市场 */
 // val SH = "S"
  /** 参数开启 */
  val YES = "1"
  /** 参数不开启 */
  val NO = "0"
  /** 买 */
  val BUY = "1"
  /** 卖 */
  val SALE = "2"

  val DEFORT_VALUE1 = "-1" //默认值1
  val DEFORT_VALUE3 = "0@0@0@0" //佣金的默认值3
  val DEFORT_VALUE2 = "0@0@0@0" //费率的默认值2
  val JSF = "JSF"
  val YHS = "YHS"
  val ZGF = "ZGF"
  val FXJ = "FXJ"
  val GHF = "GHF"
  val SXF="SXF"
  val CS1_KEY = "佣金包含经手费，证管费"
  val CS3_KEY = "是否按千分之一费率计算过户费"
  val CS4_KEY = "计算佣金减去风险金"
  val CS6_KEY = "计算佣金减去结算费"


  val CON1_KEY = "按申请编号汇总计算经手费"
  val CON2_KEY = "按申请编号汇总计算征管费"
  val CON3_KEY = "按申请编号汇总计算过户费"
  val CON4_KEY = "按申请编号汇总计算印花税"
  val CON5_KEY = "S按申请编号汇总计算佣金"
  val CON7_KEY = "S按申请编号汇总计算风险金"
  val CON8_KEY = "实际收付金额包含佣金"
  val CON11_KEY = "按成交记录计算经手费"
  val CON12_KEY = "按成交记录计算征管费"
  val CON13_KEY = "按成交记录计算过户费"
  val CON14_KEY = "按成交记录计算印花税"
  val CON15_KEY = "S按成交记录计算佣金"
  val CON17_KEY = "S按成交记录计算风险金"

  val CON18_KEY="深圳佣金计算保留位数"
  val CON19_KEY="交易所国债以全价计提佣金"
  val CON20_KEY="交易所非国债以净价计提佣金"
  val CON21_KEY="深交所证管费和经手费分别计算"
  val CON22_KEY="指数、指标股票按特殊科目设置页面处理"
  val CON23_KEY="深圳佣金计算费用保留位数"
  val CON24_KEY="深交所证管费和经手费分别计算"



}
