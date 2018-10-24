package com.yss.source.utils;

import com.linuxense.javadbf.DBFReader;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;

import java.io.FileInputStream;
import java.nio.charset.Charset;
import java.time.LocalDateTime;

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
    private int eventLines;
    private StringBuffer bodyBuffer = new StringBuffer();

    public ReadDbf(FileInputStream fileInputStream, String currentRecord, String csvSeparator, int eventLines) {
        this.reader = new DBFReader(fileInputStream);
        this.currentRecord = currentRecord;
        this.csvSeparator = csvSeparator;
        this.eventLines = eventLines;
    }

    public Event readDBFFile() {
        if (ROW == 1) {
            StringBuffer rowFirst = new StringBuffer();
            int fieldCount = reader.getFieldCount();
            for (int i = 0; i < fieldCount; i++) {
                rowFirst.append(reader.getField(i).getName());
                rowFirst.append(csvSeparator);
            }
            if (rowFirst.length() > 1) {
                rowFirst.delete(rowFirst.length() - 1, rowFirst.length());
            } else {
                System.out.println(LocalDateTime.now() + "    空白文件!");
                return null;
            }
            Event event = EventBuilder.withBody(rowFirst.toString(), Charset.forName("utf-8"));
            event.getHeaders().put(currentRecord, String.valueOf(ROW));
            ROW++;
            return event;
        } else {
            for (int a = 0; a < eventLines; a++) {
                Object[] rowValues = reader.nextRecord();
                if (rowValues != null && rowValues.length > 0) {
                    for (int i = 0; i < rowValues.length; i++) {
                        if (rowValues[i] != null) {
                            bodyBuffer.append(new String(rowValues[i].toString().getBytes(Charset.forName("8859_1")), Charset.forName("GBK")));
                            bodyBuffer.append(csvSeparator);
                        } else {
                            bodyBuffer.append(csvSeparator);
                        }
                    }
                    bodyBuffer.replace(bodyBuffer.length() - 1, bodyBuffer.length(), "\n");
                } else {
                    break;
                }
            }
            if (bodyBuffer.length() > 1) {
                bodyBuffer.delete(bodyBuffer.length() - 1, bodyBuffer.length());
            } else {
                return null;
            }
            Event event = EventBuilder.withBody(bodyBuffer.toString(), Charset.forName("utf-8"));
            event.getHeaders().put(currentRecord, String.valueOf(ROW));
            ROW++;
            bodyBuffer.setLength(0);
            return event;
        }
    }

}
