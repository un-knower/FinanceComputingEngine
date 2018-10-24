package com.yss.source.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author wangshuai
 * @version 2018-10-24 16:22
 * describe:
 * 目标文件：
 * 目标表：
 */
public class FilterFile {

    public static boolean filtration(String fileName, List<String> prefixList) {
        for (String prefix : prefixList) {
            if (fileName.toLowerCase().startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    public static List<String> assemblePrefix(String prefixStr) {
        String[] split = prefixStr.toLowerCase().split(",");
        return new ArrayList<String>(Arrays.asList(split));
    }
}
