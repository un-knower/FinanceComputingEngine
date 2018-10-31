package com.yss.source.utils;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

/**
 * @author wangshuai
 * @version 2018-10-30 10:14
 * describe:
 * 目标文件：
 * 目标表：
 */
public class ReadFsdTxt {
    private long ROW = 0;
    private RandomAccessFile randomAccessFile;
    private StringBuffer contentData = new StringBuffer();
    private String currentRecord;
    private String csvSeparator;
    private int eventLines;
    private boolean head;

    public ReadFsdTxt(RandomAccessFile randomAccessFile, String currentRecord, String csvSeparator, int eventLines, boolean head) {
        this.randomAccessFile = randomAccessFile;
        this.currentRecord = currentRecord;
        this.csvSeparator = csvSeparator;
        this.eventLines = eventLines;
        this.head = head;
    }

    /**
     * @param []
     * @return void
     * @author wangshuai
     * @date 2018/10/30 17:33
     * @description 排除前9行数据
     */
    private void excludeLineFileHead() {
        for (int a = 1; a < 10; a++) {
            try {
                randomAccessFile.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * @param []
     * @return java.lang.String
     * @author wangshuai
     * @date 2018/10/30 17:46
     * @description 读取字段信息
     */
    private String readHead() throws IOException {
        excludeLineFileHead();
        String dataLine = randomAccessFile.readLine();
        Integer integer = Integer.valueOf(dataLine);
        for (int a = 0; a < integer; a++) {
            contentData.append(randomAccessFile.readLine());
            contentData.append(csvSeparator);
        }
        if (contentData.length() > 1) {
            contentData.delete(contentData.length() - 1, contentData.length());
        } else {
            System.out.println(LocalDateTime.now() + "    空白文件!");
            return null;
        }
        ROW++;
        return contentData.toString();
    }

    private Event readContent(List<String> fieldByteList) throws IOException {
        for (int a = 0; a < eventLines; a++) {
            int b = 0;
            String rowData = randomAccessFile.readLine();
            if (rowData != null) {
                rowData = new String(rowData.getBytes("iso-8859-1"), Charset.forName("gbk"));
                byte[] gbks = rowData.getBytes("gbk");
                if (gbks.length == 8) {
//                    contentData.append(rowData);
                    Event event = EventBuilder.withBody(Transcoding.gbkToUTF(contentData.toString()));
                    event.getHeaders().put(currentRecord, String.valueOf(ROW));
                    contentData.setLength(0);
                    return event;
                } else {
                    for (String f : fieldByteList) {
                        int i = Integer.parseInt(f);
                        byte[] bytes = new byte[i];
                        if (b <= gbks.length) {
                            System.arraycopy(gbks, b, bytes, 0, i);
                            contentData.append(new String(bytes, Charset.forName("gbk")));
                            contentData.append(csvSeparator);
                            b += i;
                        } else {
                            contentData.append(csvSeparator);
                        }
                    }
                    contentData.replace(contentData.length() - 1, contentData.length(), "\n");
                    ROW++;
                }

            } else {
                System.out.println("文件读取结束");
                break;
            }
        }
        if (contentData.length() > 1) {
            contentData.delete(contentData.length() - 1, contentData.length());
        } else {
            return null;
        }
        Event event = EventBuilder.withBody(Transcoding.gbkToUTF(contentData.toString()));
        event.getHeaders().put(currentRecord, String.valueOf(ROW));
        contentData.setLength(0);
        return event;
    }

    public Event readFSDFour(List<String> fieldByteList) throws IOException {
        if (ROW == 0) {
            if (head) {
                Event event = EventBuilder.withBody(Transcoding.gbkToUTF(readHead()));
                event.getHeaders().put(currentRecord, String.valueOf(ROW));
                //跳过数据记录数
                randomAccessFile.readLine();
                contentData.setLength(0);
                return event;
            } else {
                readHead();
                randomAccessFile.readLine();
                contentData.setLength(0);
                return readContent(fieldByteList);
            }
        } else {
            return readContent(fieldByteList);
        }
    }
}
