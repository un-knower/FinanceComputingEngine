package com.yss.source.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author wangshuai
 * @version 2018-10-24 16:22
 * describe:
 * 目标文件：
 * 目标表：
 */
public class FilterFile {

    /**
     * @param [fileName, prefixList]
     * @return boolean
     * @author wangshuai
     * @date 2018/10/29 13:33
     * @description 通过前缀判断是否是DBF文件
     */
    public static boolean filtrationDbf(String fileName, List<String> prefixList) {
        for (String prefix : prefixList) {
            if (fileName.toLowerCase().startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    /**
     * @param [prefixStr]
     * @return java.util.List<java.lang.String>
     * @author wangshuai
     * @date 2018/10/29 13:34
     * @description 分隔字符串
     */
    public static List<String> assemblePrefix(String prefixStr) {

        String[] split = prefixStr.toLowerCase().split(",");
        return new ArrayList<String>(Arrays.asList(split));
    }

    /**
     * @param [regex, fileName]
     * @return boolean
     * @author wangshuai
     * @date 2018/10/29 14:24
     * @description 创建正则实例
     */
    public static boolean filtrationTxt(String fileName, String regex) {
        Pattern p = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        Matcher matcher = p.matcher(fileName);
        return matcher.matches();
    }
}
