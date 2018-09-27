package com.yss.scala.dto
/**
  * @author : 张海绥
  * @version : 2018-8-8
  *  describe: 用于生成期货成交明细元数据信息的类
  *  目标文件：09000211trddata20180420.txt
  *  目标表：QHCJMX
  */
case class QiHuoChengJiao(
                                  fdate:String,//日期
                                  accountid:String,//客户内部资金账户
                                  tradeid:String,//成交流水号
                                  instrid:String,//品种合约
                                  direction:String,//买卖标志
                                  tvolume:String,//成交量
                                  tprice:String,//成交价
                                  tamt:String,//成交额
                                  ttime:String,//成交时间
                                  offsetflag:String,//开平仓标志
                                  tzbz:String,//投机套保标志
                                  pcyk_zr:String,//平仓盈亏
                                  pcyh_zb:String,//平仓盈亏
                                  transfee:String,//手续费
                                  clientid:String,//交易编码
                                  market:String,//交易所统一标识
                                  hyflag:String,//是否为非结算会员
                                  orderid:String,//报单号
                                  userid:String,//席位号
                                  jshy:String,//监控中心编码
                                  partid:String//文件名
                                )
