package com.yss.scala.dto

/**
  * @author MingZhang Wang
  * @version 2018-09-26 14:03
  *          describe: ShenzhenStockExchangeTriPartyRepo的样例类
  *          目标文件：ShenzhenStockExchangeTriPartyRepo.scala
  *          目标表：
  */
  case class ShenzhenStockExchangeTriPartyRepoDto(
                        FDate: String, //日期
                        FinDate: String, //日期
                        FZqdm: String, //证券代码
                        FSzsh: String, //交易市场
                        FJyxwh: String, //交易席位号
                        Fje: BigDecimal, //成交金额
                        Fyj: BigDecimal, //佣金
                        Fjsf: BigDecimal, //经手费
                        FHggain: BigDecimal, //回购收益
                        FSSSFJE: BigDecimal, // 实收实付金额
                        FZqbz: String, //证券标识
                        Fjybz: String, //交易标识
                        Fjyfs: String, //交易方式
                        Fsh: Int, //审核
                        Fzzr: String, // 制作人
                        Fchk: String, // 审核人
                        FHTXH: String, //合同序号
                        FSETCODE: BigDecimal, //套账号
                        FCSGHQX: BigDecimal, //初试购回期限
                        FRZLV: BigDecimal, //融资(回购利率)
                        FSJLY: String, // 数据来源
                        FCSHTXH: String, //合同序号
                        FBS: String,
                        FSL: BigDecimal,
                        Fyhs: BigDecimal,
                        Fzgf: BigDecimal,
                        Fghf: BigDecimal,
                        FFxj: BigDecimal,
                        FQtf: BigDecimal,
                        Fgzlx: BigDecimal,
                        FQsbz: String,
                        Ftzbz: String,
                        FQsghf: String,
                        FGddm: String,
                        Fzlh: String,
                        ISRTGS: String,
                        FPARTID: String,
                        FYwbz: String,
                        Fbz: String,
                        ZqDm: String
                      )
}
