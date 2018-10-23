package com.yss.scala.core

/**
  * 上海过户的相关常量
  */
object ShghContants {

  /**分隔符 @ */
  val SEPARATE1 = "@"
  /** 分隔符 ，*/
  val SEPARATE2 = ","
  /** 分隔符 - */
  val SEPARATE3 = "-"

  /** 资产类型 GP*/
  val ZCLB = "GP"

  /** 公用资产号 0 */
  val GYZCH = "0"
  /** 市场 H*/
  val SH = "H"
  /** 通过审核 1 */
  val FSH = "1"
  /** 参数开启 1 */
  val YES = "1"
  /** 参数不开启 0*/
  val NO = "0"
  /** 买 B*/
  val BUY = "B"
  /** 卖 S*/
  val SALE = "S"


  val JSF = "JSF"
  val YHS = "YHS"
  val ZGF = "ZGF"
  val FXJ = "FXJ"
  val GHF = "GHF"
  /** 上交所A股过户费按成交金额计算*/
  val CS2_KEY = "上交所A股过户费按成交金额计算"
  /** 佣金包含经手费，证管费*/
  val CS1_KEY = "佣金包含经手费，证管费"
  /** 是否按千分之一费率计算过户费*/
  val CS3_KEY = "是否按千分之一费率计算过户费"
  /** 计算佣金减去风险金*/
  val CS4_KEY = "计算佣金减去风险金"
  /** 计算佣金减去结算费*/
  val CS6_KEY = "计算佣金减去结算费"
  /** 实际收付金额包含佣金*/
  val CON8_KEY = "实际收付金额包含佣金"
  /** 计算公共费率保留小数位 */
  val CS7_KEY = "计算公共费率保留小数位"
  /** 计算佣金保留小数位 */
  val CS8_KEY = "计算佣金保留小数位"


  /** 公共参数表 */
  val TABLE_NAME_GGCS = "LVARLIST"
  /** 基金信息表 */
  val TABLE_NAME_JJXX = "CSJJXX"
  /** 权益信息表 */
  val TABLE_NAME_QYXX = "CSQYXX"
  /** 席位表 */
  val TABLE_NAME_QSXW = "CSQSXW"
  /** 001套账的特殊科目设置表 */
   val TABLE_NAME_TSKM= "A001CSTSKM"
  /** 资产信息*/
  val TABLE_NAME_SYSJJ= "LSETCSSYSJJ"
  /** 债券信息表 */
  val TABLE_NAME_ZQXX = "CSZQXX"
  /** 股东账号表 */
  val TABLE_NAME_GDZH = "CSGDZH"
  /** 交易利率表 */
  val TATABLE_NAME_JYLV = "CSJYLV"
  /** 国债利息 */
  val TABLE_NAME_GZLX = "JJGZLX"
  /** 节假日表 */
  val TABLE_NAME_HOLIDAY = "CSHOLIDAY"
  /** 001套账的佣金利率 */
  val TABLE_NAME_A117CSJYLV = "A001CSYJLV"
  /** 特殊处理的ETF基金的业务标志和证券标志 ZQETFJY*/
  val ETF_ZQBZ_OR_YWZ = "ZQETFJY"
  /** 券商过户费 CSQSFYLV*/
  val TABLE_NAME_CSQSFYLV = "CSQSFYLV"
  /** 回购业务HG */
  val HG = "HG"
  /** 券商过户费 QSGHF */
  val QSGHF = "QSGHF"
  /** 券商过户费的ffyfs标识 0 */
  val FFYFS = "0"
  /** HDFS上源数据前缀 /yss/guzhi/interface/ */
  val PREFIX = "/yss/guzhi/interface/"

  /** 默认的将数字格式化成字符串格式 %.2f */
  val DEFAULT_DIGIT_FORMAT = "%.2f"
  /** 默认的保留的小数位 2 */
  val DEFAULT_DIGIT = 2
  /** 默认值 空串*/
  val DEFAULT_VALUE = ""
  /** 默认值 -1*/
  val DEFORT_VALUE1 = "-1"
  /** 佣金的默认值 0@0@0@0*/
  val DEFORT_VALUE3 = "0@0@0@0"
  /** 费率的默认值 0@0@0*/
  val DEFORT_VALUE2 = "0@0@0"
  /** 默认保留的小数位 2*/
  val DEFORT_ROUND = "2"
  /** 默认值 -1@-1 */
  val DEFAULT_VALUE4 = "-1@-1"

  /** 债券类型取债券品种信息维护的债券类型 */
  val CON01_KEY = "债券类型取债券品种信息维护的债券类型"
  /** 指数、指标股票按特殊科目设置页面处理 */
  val CON02_KEY = "指数、指标股票按特殊科目设置页面处理"
  /** 上交所是否启用企债净价交易 */
  val CON03_KEY = "上交所是否启用企债净价交易"
  /** 上海回购价格位数 */
  val CON04_KEY = "上海回购价格位数"

  /** 上海过户的可能没有用到 */
  val CON1_KEY = "按申请编号汇总计算经手费"
  val CON2_KEY = "按申请编号汇总计算征管费"
  val CON3_KEY = "按申请编号汇总计算过户费"
  val CON4_KEY = "按申请编号汇总计算印花税"
  val CON5_KEY = "H按申请编号汇总计算佣金"
  val CON7_KEY = "H按申请编号汇总计算风险金"
  val CON11_KEY = "按成交记录计算经手费"
  val CON12_KEY = "按成交记录计算征管费"
  val CON13_KEY = "按成交记录计算过户费"
  val CON14_KEY = "按成交记录计算印花税"
  val CON15_KEY = "H按成交记录计算佣金"
  val CON17_KEY = "H按成交记录计算风险金"

}
