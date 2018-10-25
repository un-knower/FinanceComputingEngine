package com.yss.source.utils;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import java.nio.charset.Charset;
import java.time.LocalDateTime;

/**
 * @author wangshuai
 * @version 2018-10-24 09:00
 * describe:
 * 目标文件：
 * 目标表：
 */
public class ReadXlsx {
    private int RowIndex;
    private static StringBuffer buffer = new StringBuffer();
    private String currentRecord;
    private String csvSeparator;
    private Sheet sheet;
    private int eventLines;
    private final int lastRowIndex;

    public ReadXlsx(Workbook wb, String currentRecord, String csvSeparator, int eventLines, Boolean head) {
        this.currentRecord = currentRecord;
        this.csvSeparator = csvSeparator;
        this.eventLines = eventLines;
        this.sheet = wb.getSheetAt(0);
        this.lastRowIndex = this.sheet.getLastRowNum();
        //ture取头数据   false 不取头数据
        if (head) {
            this.RowIndex = this.sheet.getFirstRowNum();
        } else {
            this.RowIndex = this.sheet.getFirstRowNum() + 1;
        }

    }

    private void readRow() {
        if (RowIndex <= lastRowIndex) {
            Row row = sheet.getRow(RowIndex);
            if (row != null) {
                int firstCellIndex = row.getFirstCellNum();
                int lastCellIndex = row.getLastCellNum();
                for (int cIndex = firstCellIndex; cIndex < lastCellIndex; cIndex++) {   //遍历列
                    Cell cell = row.getCell(cIndex);
                    if (cell != null) {
                        buffer.append(cell.toString().replaceAll("\n", "   "));
                        buffer.append(csvSeparator);
                    } else {
                        buffer.append(csvSeparator);
                    }
                }
                buffer.replace(buffer.length() - 1, buffer.length(), "\n");
            }
        }
        RowIndex++;
    }


    public Event readRows() {
        if (RowIndex == sheet.getFirstRowNum()) {
            readRow();
            if (buffer.length() > 1) {
                buffer.delete(buffer.length() - 1, buffer.length());
            } else {
                System.out.println(LocalDateTime.now() + "    空白文件!");
                return null;
            }
            Event event = EventBuilder.withBody(buffer.toString(), Charset.forName("utf-8"));
            event.getHeaders().put(currentRecord, String.valueOf(RowIndex));
            buffer.setLength(0);
            return event;
        } else {
            for (int a = 0; a < eventLines; a++) {
                readRow();
            }
            if (buffer.length() > 1) {
                buffer.delete(buffer.length() - 1, buffer.length());
            } else {
                return null;
            }
            Event event = EventBuilder.withBody(buffer.toString(), Charset.forName("utf-8"));
            event.getHeaders().put(currentRecord, String.valueOf(RowIndex));
            buffer.setLength(0);
            return event;
        }
    }
}