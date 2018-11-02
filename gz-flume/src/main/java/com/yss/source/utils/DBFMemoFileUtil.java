package com.yss.source.utils;

import com.linuxense.javadbf.DBFDataType;
import com.linuxense.javadbf.DBFMemoFile;

import java.io.File;
import java.nio.charset.Charset;

/**
 * @author wangshuai
 * @version 2018-11-02 13:56
 * describe:
 * 目标文件：
 * 目标表：
 */
public class DBFMemoFileUtil extends DBFMemoFile {
    protected DBFMemoFileUtil(File memoFile, Charset charset) {
        super(memoFile, charset);
    }

    @Override
    protected Object readData(int block, DBFDataType type) {
        return super.readData(block, type);
    }
}
