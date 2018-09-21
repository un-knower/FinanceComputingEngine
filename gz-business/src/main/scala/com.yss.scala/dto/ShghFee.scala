package com.yss.scala.dto

/**
  * ctype按何种方式计算的费率
  * 取值1：每一笔交易单独计算，最后相加
  * 取值2：相同申请编号的金额汇总*费率，各申请编号汇总后的金额相加
  * 取值3：金额汇总*费率
  *费率参数
  */
case class ShghFee(ctype: String, sumCjje: BigDecimal, sumCjsl: BigDecimal, sumYj: BigDecimal, sumJsf: BigDecimal, sumYhs: BigDecimal, sumZgf: BigDecimal,
                   sumGhf: BigDecimal, sumFxj: BigDecimal,sumGzlx:BigDecimal,sumHgsy:BigDecimal)