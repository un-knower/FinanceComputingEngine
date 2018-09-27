****************************************************************************************

*** 说明文档: oracle表对应hdfs路径及表说明

*** 编 写 人:  MingZhang Wong

*** 编写日期:  2018-09-27

*** 修 改 人:

*** 修改日期:

*** 备    注:

*****************************************************************************************

# 基础信息表表信息

    lsetlist  资产信息表
    csgdzh    股东账号表
    csqsxw    席位表
    lvarlist  参数表
    csjylv    费用利率表
    CsJjXx    基金信息表
    cszqxx    债券信息表
    a117csyjlv 佣金利率表
    a117CsTsKm 特殊科目设置表

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