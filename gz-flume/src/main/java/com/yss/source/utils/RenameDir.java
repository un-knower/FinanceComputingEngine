package com.yss.source.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Event;

import java.io.File;
import java.util.HashMap;

/**
 * @author wangshuai
 * @version 2018-11-05 09:55
 * describe:
 * 目标文件：
 * 目标表：
 */
public class RenameDir {
    /**
     * @param [event, filePath, fileNameHeader]
     * @return void
     * @author wangshuai
     * @date 2018/11/6 13:16
     * @description 添加文件夹
     */
    public void rename(Event event, String filePath, String fileNameHeader) {

        HashMap<String, String> hashMap = getFileRegexMap();
        File file = new File(filePath);
        String[] split = filePath.split("/");
        if (split.length > 2) {
            for (String regex : hashMap.keySet()) {
                if (FilterFile.filtrationTxt(file.getName(), regex)) {
                    split[1] = split[1] + "/" + hashMap.get(regex);
                    event.getHeaders().put(fileNameHeader, StringUtils.join(split, "/"));
                    break;
                }
            }
        } else {
            for (String regex : hashMap.keySet()) {
                if (FilterFile.filtrationTxt(file.getName(), regex)) {
                    String fileName = hashMap.get(regex) + "/" + filePath;
                    event.getHeaders().put(fileNameHeader, fileName);
                    break;
                }
            }
        }

    }

    private static HashMap<String, String> getFileRegexMap() {
        //key 正则表达式    value 文件夹
        HashMap<String, String> hashMap = new HashMap<>();
        //FSD文件
        hashMap.put("^FSD_\\d*_\\d*_\\d*_04_\\d*_\\d*\\.txt$", "fsd04");//中登FISP交易确认
        hashMap.put("^FSD_\\d*_\\d*_\\d*_06_\\d*_\\d*\\.txt$", "fsd06");//中登FISP分红文件
        hashMap.put("^FSD_\\d*_\\d*_\\d*_JY\\.TXT$", "fsdjy");//中登FISP交易汇总
        //分隔符是*的
        hashMap.put("^\\d*delivdetails\\d*\\.txt$", "delivdetails");//商品期货交割数据文件
        hashMap.put("^\\d*holddata\\d*\\.txt$", "holddata");//商品期货持仓数据文件
        hashMap.put("^\\d*trddata\\d*\\.txt$", "trddata");//成交明细文件
        hashMap.put("^\\d*otherfund\\d*\\.txt$", "otherfund");//其他分项资金数据文件
        hashMap.put("^\\d*fundchg\\d*\\.txt$", "fundchg");//客户出入金记录文件
        hashMap.put("^\\d*cusfund\\d*\\.txt$", "cusfund");//客户基本资金数据文件
        //分隔符是|的
        hashMap.put("^hkexclpr04_\\d*\\.txt$", "hkexclpr04");//港股收盘价格文件（深圳）
        hashMap.put("^hkexreff04_\\d*\\.txt$", "hkexreff04");//港股产品信息（深圳）
        hashMap.put("^reff04\\d*\\.txt$", "reff04");//港股产品信息（深圳）
        hashMap.put("^reff03\\d*\\.txt$", "reff03");//期权基础信息
        hashMap.put("^cpxx\\d*.txt$", "cpxx");//上海产品信息文件接口
        hashMap.put("^[a-z].*\\d{6}[a-z].*\\d*00SPOTMATCH\\.txt$", "spotmatch");//现货成交单数据文件
        hashMap.put("^[a-z].*\\d{6}[a-z].*\\d*00DEFERMATCH\\.txt$", "defermatch");//递延成交单数据文件
        hashMap.put("^[a-z].*\\d{6}[a-z].*\\d*00DEFERDELIVERYAPPMATCH\\.txt$", "deferdeliveryappmatch");//交割申报成交单数据文件
        hashMap.put("^[a-z].*\\d{6}[a-z].*\\d*00CLIENTSTORAGEFLOW\\.txt$", "clientstorageflow");//客户库存变化流水数据文件
        hashMap.put("^[a-z].*\\d{6}[a-z].*\\d*00ETFAPPLY\\.txt$", "etfapply");//认申赎清单数据文件
        hashMap.put("^[a-z].*\\d{6}[a-z].*\\d*00MEMBERSEATCAPICATL\\.txt$", "memberseatcapicatl");//席位资金数据文件
        hashMap.put("^[a-z].*\\d{6}[a-z].*\\d*00CLIENTMISFEEDETAIL\\.txt$", "clientmisfeedetail");//客户费用数据文件
        hashMap.put("^[a-z].*\\d{6}[a-z].*\\d*00CLIENTSTORAGE\\.txt$", "clientstorage");//客户库存变化流水数据文件
        hashMap.put("^[a-z].*\\d{6}[a-z].*\\d*00LARGEAMOUNTMATCH\\.txt$", "largeamountmatch");//大宗交易成交单数据文件
        //通过前缀匹配dbf文件
        hashMap.put("^gzlx\\..*$", "gzlx");//上海国债利息库
        hashMap.put("^zqye.*$", "zqye");//上海证券余额库
        hashMap.put("^qtsl.*$", "qtsl");//上海其他数量库
        hashMap.put("^zqbd.*$", "zqbd");//港股通证券变动文件
        hashMap.put("^zqjsxx.*$", "zqjsxx");//上海证券结算信息库
        hashMap.put("^hk_jsmx.*$", "hkjsmx");//港股通结算明细文件
        hashMap.put("^hk_tzxx.*$", "hktzxx");//港股通知信息文件
        hashMap.put("^hk_zqbd.*$", "hkzqbd");//港股通证券变动文件
        hashMap.put("^hk_zqye.*$", "hkzqye");//港股通证券余额对账文件
        hashMap.put("^hk_ckhl.*$", "hkckhl");//港股通参考汇率文件
        hashMap.put("^abcsj.*$", "abcsj");//股息红利差异化计税需要补缴情况明细数据
        hashMap.put("^ywhb.*$", "ywhb");//业务汇报文件
        hashMap.put("^zjhz.*$", "zjhz");//上海资金汇总结算
        hashMap.put("^wdq.*$", "wdq");//未到期业务对账文件
        hashMap.put("^hk_zjbd.*$", "hkzjbd");//港股通资金变动文件
        hashMap.put("^hk_zjhz.*$", "hkzjhz");//港股通资金汇总文件
        hashMap.put("^hk_zjye.*$", "hkzjye");//港股通资金余额文件
        hashMap.put("^jsmxjs.*$", "jsmxjs");//上海结算明细库
        hashMap.put("^op_bzjzh.*$", "opbzjzh");//衍生品保证金账户数据
        //BDF文件
        hashMap.put("^gh\\d*\\.dbf$", "gh");//上海回报库
        hashMap.put("^BGH\\d*\\.dbf$", "bgh");//上海综合业务回报库
        hashMap.put("^SJSHB\\.dbf$", "sjshb");//深圳回报库
        hashMap.put("^SJSTJ\\d*\\.dbf$", "sjstj");//深圳统计库
        hashMap.put("^SJSFX\\.dbf$", "sjsfx");//深圳发行库
        hashMap.put("^SJSXX\\.dbf$", "sjsxx");//深圳信息库
        hashMap.put("^SJSHQ\\.dbf$", "sjshq");//深圳行情库
        hashMap.put("^DGH\\.dbf$", "dgh");//上海大宗交易库
        hashMap.put("^BJZXYH\\.dbf$", "bjzxyh");//固定收益证券成交库
        hashMap.put("^SJSFW\\.dbf$", "sjsfw");//深圳服务库
        hashMap.put("^\\d*_.*_\\d*_\\d*_TRADE\\.dbf$", "trade");//中金所成交单
        hashMap.put("^\\d*_.*_\\d*_\\d*_SETTLEMENTDETAIL\\.dbf$", "settlementdetail");//中金所标准合约结算表
        hashMap.put("^\\d*_.*_\\d*_\\d*_DELIVERY\\.dbf$", "delivery");//中金所交割单
        hashMap.put("^SJSJG\\.dbf$", "sjsjg");//深交所结果库
        hashMap.put("^HK_QTSL\\.dbf$", "hkqtsl");//港股通证券其他数量对账文件
        hashMap.put("^HK_YWHB\\.dbf$", "hkywhb");//港股通业务回报文件
        hashMap.put("^SZHK_SJSMX1\\d*\\.dbf$", "szhksjsmx1");//港股通（深圳）交易清算明细文件
        hashMap.put("^SZHK_SJSMX2\\d*\\.dbf$", "szhksjsmx2");//港股通（深圳）组合费清算明细文件
        hashMap.put("^SZHK_QTSL\\d*\\.dbf$", "szhkqtsl");//港股通（深圳）证券其他数量文件
        hashMap.put("^SZHK_TZXX\\d*\\.dbf$", "szhktzxx");//港股通（深圳）通知信息文件
        hashMap.put("^SZHK_SJSJG\\d*\\.dbf$", "szhksjsjg");//港股通（深圳）证券变动文件
        hashMap.put("^ZSMXFK\\d*\\.dbf$", "zsmxfk");//征税明细反馈库
        hashMap.put("^ZSMX\\.dbf$", "zsmx");//征税明细信息库
        hashMap.put("^SJSMX2\\.dbf$", "sjsmx");//深交所清算明细库
        hashMap.put("^SJSMX1\\.dbf$", "sjsmx");//深交所清算明细库
        hashMap.put("^SJSMX0\\.dbf$", "sjsmx");//深交所清算明细库
        hashMap.put("^SJSZHHB\\.dbf$", "sjszhhb");//深交所综合回报库
        hashMap.put("^LOFMXZF\\.dbf$", "lofmxzf");//上证LOF清算明细文件
        hashMap.put("^LOFJS\\.dbf$", "lofjs");//深圳LOF结算信息库
        hashMap.put("^OP_JSMX\\.dbf$", "opjsmx");//期权结算明细文件
        hashMap.put("^OP_CCBD\\.dbf$", "opccbd");//期权合约持仓变动数据
        hashMap.put("^NQHB\\.dbf$", "nqhb");//股份转让系统回报库
        hashMap.put("^BJSJG\\.dbf$", "bjsjg");//股份转让系统结果库
        hashMap.put("^execution_aggr_.*_\\d*_\\d*\\.tsv$", "execution");//深交所V5接口成交汇总
        hashMap.put("^SJSZJ\\d*\\.dbf$", "sjszj");//深圳资金汇总结算
        hashMap.put("^SJSQS\\d*\\.dbf$", "sjsqs");//深圳清算库
        hashMap.put("^SJSGB\\.dbf$", "sjsgb");//深圳广播类信息库
        hashMap.put("^SZHK_SJSDZ\\d*\\.dbf$", "szhksjsdz");//港股通（深圳）证券余额对账文件
        hashMap.put("^GLRHB_\\d*_\\d*_\\d*_.*\\.dbf$", "glrhb");//管理人回报库
        hashMap.put("^ZRTBDQXFL\\.dbf$", "zrtbdqxfl");//转融通标的期限费率库
        hashMap.put("^\\d*_ZRTQYCLK\\.dbf$", "zrtqyclk");//转融通权益处理库
        hashMap.put("^\\d*_ZRTXHYXX\\.dbf$", "zrtxhyxx");//转融通新合约信息库
        hashMap.put("^\\d*_ZRTHYCHMX\\.dbf$", "zrthychmx");//T日合约偿还明细库
        hashMap.put("^RZRQJY_\\d*\\.dbf$", "rzrqjy");//融资融券交易数据
        hashMap.put("^BJZSMXFK\\.dbf", "bjzsmxfk");//股份转让征税明细反馈库
        hashMap.put("^BJZSMX\\.dbf", "bjzsmx");//股份转让征税明细信息库
        hashMap.put("^RZRQWJLX_\\d*_\\d*\\.dbf", "rzrqwjlx");//海通融资融券每日利息文件
        hashMap.put("^RZRQJS_\\d*_\\d*\\.dbf", "rzrqjs");//海通融资融券交易文件
        hashMap.put("^TESTPRE_\\d*_\\d*\\.dbf", "testpre");//海通融资融券交易文件
        hashMap.put("^NQXX\\.dbf", "nqxx");//股份转让系统证券信息库
        hashMap.put("^HLSA\\d*\\.dbf", "hlsa");//A股股息红利差异化计税缴纳成功申报明细数据
        hashMap.put("^SJSGF\\.dbf", "SJSGF");//深圳股份库
        hashMap.put("^SJSDZ\\.dbf", "sjsdz");//深圳对账库
        hashMap.put("^RZRQLX_\\d*\\.dbf", "rzrqlx");//融资融券每日利息
        hashMap.put("^\\d*_.*_\\d*_\\d_ClientCapitalDetail\\.dbf", "clientcapitaldetail");//客户分项资金明细表
        return hashMap;
    }

}
