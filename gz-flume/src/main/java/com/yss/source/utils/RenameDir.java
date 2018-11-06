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
        hashMap.put("^FSD_\\d*_\\d*_\\d*_04_\\d*_\\d*\\.txt$", "fsd04");
        hashMap.put("^FSD_\\d*_\\d*_\\d*_06_\\d*_\\d*\\.txt$", "fsd06");
        hashMap.put("^FSD_\\d*_\\d*_\\d*_JY\\.TXT$", "fsdjy");
        //分隔符是*的
        hashMap.put("^\\d*delivdetails\\d*\\.txt$", "delivdetails");
        hashMap.put("^\\d*holddata\\d*\\.txt$", "holddata");
        hashMap.put("^\\d*trddata\\d*\\.txt$", "trddata");
        hashMap.put("^\\d*otherfund\\d*\\.txt$", "otherfund");
        hashMap.put("^\\d*fundchg\\d*\\.txt$", "fundchg");
        hashMap.put("^\\d*cusfund\\d*\\.txt$", "cusfund");
        //分隔符是|的
        hashMap.put("^hkexclpr04_\\d*\\.txt$", "hkexclpr04");
        hashMap.put("^hkexreff04_\\d*\\.txt$", "hkexreff04");
        hashMap.put("^reff04\\d*\\.txt$", "reff04");
        hashMap.put("^reff03\\d*\\.txt$", "reff03");
        hashMap.put("^cpxx\\d*.txt$", "cpxx");
        hashMap.put("^[a-z].*\\d{6}[a-z].*\\d*00SPOTMATCH\\.txt$", "spotmatch");
        hashMap.put("^[a-z].*\\d{6}[a-z].*\\d*00DEFERMATCH\\.txt$", "defermatch");
        hashMap.put("^[a-z].*\\d{6}[a-z].*\\d*00DEFERDELIVERYAPPMATCH\\.txt$", "deferdeliveryappmatch");
        hashMap.put("^[a-z].*\\d{6}[a-z].*\\d*00CLIENTSTORAGEFLOW\\.txt$", "clientstorageflow");
        hashMap.put("^[a-z].*\\d{6}[a-z].*\\d*00ETFAPPLY\\.txt$", "etfapply");
        hashMap.put("^[a-z].*\\d{6}[a-z].*\\d*00MEMBERSEATCAPICATL\\.txt$", "memberseatcapicatl");
        hashMap.put("^[a-z].*\\d{6}[a-z].*\\d*00CLIENTMISFEEDETAIL\\.txt$", "clientmisfeedetail");
        hashMap.put("^[a-z].*\\d{6}[a-z].*\\d*00CLIENTSTORAGE\\.txt$", "clientstorage");
        hashMap.put("^[a-z].*\\d{6}[a-z].*\\d*00LARGEAMOUNTMATCH\\.txt$", "largeamountmatch");
        //通过前缀匹配dbf文件
        hashMap.put("^gzlx\\..*$", "gzlx");
        hashMap.put("^zqye.*$", "zqye");
        hashMap.put("^qtsl.*$", "qtsl");
        hashMap.put("^zqbd.*$", "zqbd");
        hashMap.put("^zqjsxx.*$", "zqjsxx");
        hashMap.put("^hk_jsmx.*$", "hk_jsmx");
        hashMap.put("^hk_tzxx.*$", "hk_tzxx");
        hashMap.put("^hk_zqbd.*$", "hk_zqbd");
        hashMap.put("^hk_zqye.*$", "hk_zqye");
        hashMap.put("^hk_ckhl.*$", "hk_ckhl");
        hashMap.put("^abcsj.*$", "abcsj");
        hashMap.put("^ywhb.*$", "ywhb");
        hashMap.put("^zjhz.*$", "zjhz");
        hashMap.put("^wdq.*$", "wdq");
        hashMap.put("^hk_zjbd.*$", "hk_zjbd");
        hashMap.put("^hk_zjhz.*$", "hk_zjhz");
        hashMap.put("^hk_zjye.*$", "hk_zjye");
        hashMap.put("^jsmxjs.*$", "jsmxjs");
        hashMap.put("^op_bzjzh.*$", "op_bzjzh");
        //BDF文件
        hashMap.put("^gh\\d*\\.dbf$", "gh");
        hashMap.put("^BGH\\d*\\.dbf$", "bgh");
        hashMap.put("^SJSHB\\.dbf$", "sjshb");
        hashMap.put("^SJSTJ\\d*\\.dbf$", "sjstj");
        hashMap.put("^SJSFX\\.dbf$", "sjsfx");
        hashMap.put("^SJSXX\\.dbf$", "sjsxx");
        hashMap.put("^SJSHQ\\.dbf$", "sjshq");
        hashMap.put("^DGH\\.dbf$", "dgh");
        hashMap.put("^BJZXYH\\.dbf$", "bjzxyh");
        hashMap.put("^SJSFW\\.dbf$", "sjsfw");
        hashMap.put("^\\d*_.*_\\d*_\\d*_TRADE\\.dbf$", "trade");
        hashMap.put("^\\d*_.*_\\d*_\\d*_SETTLEMENTDETAIL\\.dbf$", "settlementdetail");
        hashMap.put("^\\d*_.*_\\d*_\\d*_DELIVERY\\.dbf$", "delivery");
        hashMap.put("^SJSJG\\.dbf$", "sjsjg");
        hashMap.put("^HK_QTSL\\.dbf$", "hkqtsl");
        hashMap.put("^HK_YWHB\\.dbf$", "hkywhb");
        hashMap.put("^SZHK_SJSMX1\\d*\\.dbf$", "szhksjsmx");
        hashMap.put("^SZHK_SJSMX2\\d*\\.dbf$", "szhksjsmx");
        hashMap.put("^SZHK_QTSL\\d*\\.dbf$", "szhkqtsl");
        hashMap.put("^SZHK_TZXX\\d*\\.dbf$", "szhktzxx");
        hashMap.put("^SZHK_SJSJG\\d*\\.dbf$", "szhksjsjg");
        hashMap.put("^ZSMXFK\\d*\\.dbf$", "zsmxfk");
        hashMap.put("^ZSMX\\.dbf$", "zsmx");
        hashMap.put("^SJSMX2\\.dbf$", "sjsmx");
        hashMap.put("^SJSMX1\\.dbf$", "sjsmx");
        hashMap.put("^SJSMX0\\.dbf$", "sjsmx");
        hashMap.put("^SJSZHHB\\.dbf$", "sjszhhb");
        hashMap.put("^LOFMXZF\\.dbf$", "lofmxzf");
        hashMap.put("^LOFJS\\.dbf$", "lofjs");
        hashMap.put("^OP_JSMX\\.dbf$", "opjsmx");
        hashMap.put("^OP_CCBD\\.dbf$", "opccbd");
        hashMap.put("^NQHB\\.dbf$", "nqhb");
        hashMap.put("^BJSJG\\.dbf$", "bjsjg");
        hashMap.put("^execution_aggr_.*_\\d*_\\d*\\.tsv$", "execution");
        hashMap.put("^SJSZJ\\d*\\.dbf$", "sjszj");
        hashMap.put("^SJSQS\\d*\\.dbf$", "sjsqs");
        hashMap.put("^SJSGB\\.dbf$", "sjsgb");
        hashMap.put("^SZHK_SJSDZ\\d*\\.dbf$", "szhksjsdz");
        hashMap.put("^GLRHB_\\d*_\\d*_\\d*_.*\\.dbf$", "glrhb");
        hashMap.put("^ZRTBDQXFL\\.dbf$", "zrtbdqxfl");
        hashMap.put("^\\d*_ZRTQYCLK\\.dbf$", "zrtqyclk");
        hashMap.put("^\\d*_ZRTXHYXX\\.dbf$", "zrtxhyxx");
        hashMap.put("^\\d*_ZRTHYCHMX\\.dbf$", "zrthychmx");
        hashMap.put("^RZRQJY_\\d*\\.dbf$", "rzrqjy");
        hashMap.put("^BJZSMXFK\\.dbf", "bjzsmxfk");
        hashMap.put("^BJZSMX\\.dbf", "bjzsmx");
        hashMap.put("^RZRQWJLX_\\d*_\\d*\\.dbf", "rzrqwjlx");
        hashMap.put("^RZRQJS_\\d*_\\d*\\.dbf", "rzrqjs");
        hashMap.put("^TESTPRE_\\d*_\\d*\\.dbf", "testpre");
        hashMap.put("^NQXX\\.dbf", "nqxx");
        hashMap.put("^HLSA\\d*\\.dbf", "hlsa");
        hashMap.put("^SJSGF\\.dbf", "SJSGF");
        hashMap.put("^SJSDZ\\.dbf", "sjsdz");
        hashMap.put("^RZRQLX_\\d*\\.dbf", "rzrqlx");
        hashMap.put("^\\d*_.*_\\d*_\\d_ClientCapitalDetail\\.dbf", "clientcapitaldetail");
        return hashMap;
    }

}
