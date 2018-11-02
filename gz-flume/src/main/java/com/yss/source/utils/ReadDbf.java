package com.yss.source.utils;

import com.linuxense.javadbf.DBFReader;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;

import java.io.FileInputStream;
import java.time.LocalDateTime;

/**
 * @author wangshuai
 * @version 2018-09-29 16:56
 * describe:
 * 目标文件：
 * 目标表：
 */
public class ReadDbf {
    private long ROW;
    private DBFReadUtil reader;
    private String currentRecord;
    private String csvSeparator;
    private int eventLines;
    private StringBuffer bodyBuffer = new StringBuffer();

    public ReadDbf(FileInputStream fileInputStream, String currentRecord, String csvSeparator, int eventLines, boolean head) {
        this.reader = new DBFReadUtil(fileInputStream);
        this.currentRecord = currentRecord;
        this.csvSeparator = csvSeparator;
        this.eventLines = eventLines;
        //ture取头数据   false 不取头数据
        if (head) {
            this.ROW = 1;
        } else {
            this.ROW = 2;
        }
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

            Event event = EventBuilder.withBody(Transcoding.transcodByte(rowFirst.toString()));
            event.getHeaders().put(currentRecord, String.valueOf(ROW));
            ROW++;
            return event;
        } else {
            for (int a = 0; a < eventLines; a++) {
                Object[] rowValues = reader.nextRecord();
                if (rowValues != null && rowValues.length > 0) {
                    for (int i = 0; i < rowValues.length; i++) {
                        if (rowValues[i] != null) {
                            bodyBuffer.append(rowValues[i].toString());
                            bodyBuffer.append(csvSeparator);
                        } else {
                            bodyBuffer.append(csvSeparator);
                        }
                    }
                    bodyBuffer.replace(bodyBuffer.length() - 1, bodyBuffer.length(), "\n");
                } else {
                    break;
                }
                ROW++;
            }
            if (bodyBuffer.length() > 1) {
                bodyBuffer.delete(bodyBuffer.length() - 1, bodyBuffer.length());
            } else {
                return null;
            }
            Event event = EventBuilder.withBody(Transcoding.transcodByte(bodyBuffer.toString()));
            event.getHeaders().put(currentRecord, String.valueOf(ROW));
            bodyBuffer.setLength(0);
            return event;
        }
    }

}
