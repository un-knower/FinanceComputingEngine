package com.yss.source.utils;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

/**
 * @author wangshuai
 * @version 2018-10-29 17:50
 * describe:
 * 目标文件：
 * 目标表：
 */
public class Transcoding {
    public static String transcod(String data) {
        String s = null;
        try {
            s = new String(data.getBytes("ISO-8859-1"), Charset.forName("gbk"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            System.out.println("转码出现异常!");
        }
        return new String(s.getBytes(), Charset.forName("utf-8"));
    }

    public static byte[] transcodByte(String data) {
        String s = null;
        try {
            s = new String(data.getBytes("ISO-8859-1"), Charset.forName("gbk"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            System.out.println("转码出现异常!");
        }
        try {
            return s.getBytes("utf-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            System.out.println("转码出现异常!");
        }
        return null;
    }
}
