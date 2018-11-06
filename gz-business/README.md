****************************************************************************************

*** 说明文档: oracle表对应hdfs路径及表说明

*** 编 写 人:  MingZhang Wong

*** 编写日期:  2018-09-27

*** 修 改 人:  MingZhang Wong

*** 修改日期:  2018-11-06

*** 备    注:  更新表信息

*****************************************************************************************
# 类名命名（参照说明） 

https://docs.qq.com/sheet/DZ3BvYkpLa1ZrUFNV?opendocxfrom=admin&tab=BB08J2

# 基础信息表表信息


    CSGDZH      股东账号表
    CSHOLIDAY   节假日信息表
    CSJJXX      基金信息表
    CSJYLV      交易费率表
    CSQSFYLV    券商费用利率表
    CSQSXW      券商席位表
    CSZQXX      债券信息表
    CSSYSTSKM   特殊科目表
    CSSYSYJLV   券商佣金利率表
    CSZQXX      券商品种信息表
    HK_TZXX     汇总接口清算表
    JJGZLX      国债利息表  
    LSETCSSYSJJ 基金基本参数集合表
    LSETLIST    基金资产信息表
    LVARLIST    参数信息表

## 基础信息表存储路径
    路径为：/yss/guzhi/basic_list/当天日期
    
    http://192.168.102.120:50070/explorer.html#/yss/guzhi/basic_list/

# HDFS 路径规则
    ${date}格式，例如20180917
    hdfs://hostname:9000/yss/guzhi/interface/${date}/接口名称/数据文件
    hdfs://hostname:9000/yss/guzhi/base_list/${date}/表名/数据文件

## 估值核算原数据
    ip:  192.168.102.68
    sid:  orcl
    user:hadoop
    pass:hadoop