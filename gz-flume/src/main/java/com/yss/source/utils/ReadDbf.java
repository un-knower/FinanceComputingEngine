package com.yss.source.utils;

import com.linuxense.javadbf.DBFReader;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;

import java.io.FileInputStream;
import java.nio.charset.Charset;

/**
 * @author wangshuai
 * @version 2018-09-29 16:56
 * describe:
 * 目标文件：
 * 目标表：
 */
public class ReadDbf {
    private long ROW = 1;
    private DBFReader reader;
    private String currentRecord;
    private String csvSeparator;

    public ReadDbf(FileInputStream fileInputStream, String currentRecord, String csvSeparator) {
        this.reader = new DBFReader(fileInputStream);
        this.currentRecord = currentRecord;
        this.csvSeparator = csvSeparator;
    }

    public Event readDBF() {

        if (ROW == 1) {
            StringBuffer rowFirst = new StringBuffer();
            int fieldCount = reader.getFieldCount();
            for (int i = 0; i < fieldCount; i++) {
                rowFirst.append(reader.getField(i).getName());
                rowFirst.append(csvSeparator);
            }
            rowFirst.delete(rowFirst.length() - 1, rowFirst.length());
            Event event = EventBuilder.withBody(rowFirst.toString(), Charset.forName("utf-8"));
            event.getHeaders().put(currentRecord, String.valueOf(ROW));
            ROW++;
            return event;
        } else {
            StringBuffer bodyBuffer = new StringBuffer();
            Object[] rowValues = reader.nextRecord();
            if (rowValues != null && rowValues.length > 0) {
                for (int i = 0; i < rowValues.length; i++) {
                    if (rowValues[i] != null) {
                        bodyBuffer.append(new String(rowValues[i].toString().getBytes(Charset.forName("8859_1")), Charset.forName("GBK")));
                        bodyBuffer.append(csvSeparator);
                    } else {
                        return null;
                    }
                }
                bodyBuffer.delete(bodyBuffer.length() - 1, bodyBuffer.length());
                Event event = EventBuilder.withBody(bodyBuffer.toString(), Charset.forName("utf-8"));
                event.getHeaders().put(currentRecord, String.valueOf(ROW));
                ROW++;
                return event;
            } else {
                return null;
            }
        }
    }

}
