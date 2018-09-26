/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.yss.taildirsource;

import com.google.common.collect.Lists;
import com.linuxense.javadbf.DBFReader;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.hadoop.io.DataOutputBuffer;
import org.json.CDL;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.XML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.yss.taildirsource.TaildirSourceConfigurationConstants.BYTE_OFFSET_HEADER_KEY;


public class TailFile {
    private static final Logger logger = LoggerFactory.getLogger(TailFile.class);
    //换行键
    private static final byte BYTE_NL = (byte) 10;
    //回车键
    private static final byte BYTE_CR = (byte) 13;
    //每次读的大小
    private static final int BUFFER_SIZE = 8192;
    private static final int NEED_READING = -1;

    private RandomAccessFile raf;
    private final String path;
    private final long inode;
    private long pos;
    private long lastUpdated;
    private boolean needTail;
    private final Map<String, String> headers;
    private byte[] buffer;
    private byte[] oldBuffer;
    private int bufferPos;
    private long lineReadPos;

    private File filePath;

    private String fileName;
    private String parentDir;


    /*=================================DBF分割线=====开始=======================================*/
    private InputStream fis;
    private DBFReader reader;
    private long dbfRow = 0;
    private int xmlRow = 0;
    private String startLabel = "Security";

    /*=================================DBF分割线======结束======================================*/

    /*------------------XML分割线------开始-------------------*/
    private byte[] startTag = ("<" + startLabel + ">").getBytes(Charset.forName("utf-8"));
    private byte[] currentStartTag;
    private byte[] endTag = ("</" + startLabel + ">").getBytes(Charset.forName("utf-8"));
    private byte[] endEmptyTag = "/>".getBytes(Charset.forName("utf-8"));
    private byte[] space = " ".getBytes(Charset.forName("utf-8"));
    private byte[] angleBracket = ">".getBytes(Charset.forName("utf-8"));
    private Long start;
    private Long end;
    private DataOutputBuffer databuffer = new DataOutputBuffer();


    public String xmlNext() throws IOException {
        if (readUntilStartElement()) {
            try {
                databuffer.write(currentStartTag);
                if (readUntilEndElement()) {
                    JSONObject nObject = XML.toJSONObject(new String(databuffer.getData(), 0, databuffer.getLength()));
                    return nObject.get(startLabel).toString();
                } else {
                    return null;
                }
            } finally {
                databuffer.reset();
            }
        } else {
            return null;
        }
    }

    public boolean readUntilStartElement() throws IOException {
        currentStartTag = startTag;
        int i = 0;
        while (true) {
            int b = raf.read();
            if (b == -1 || (i == 0 && raf.getFilePointer() > end)) {
                // End of file or end of split.
                return false;
            } else {
                if (b == startTag[i]) {
                    if (i >= startTag.length - 1) {
                        // Found start tag.
                        return true;
                    } else {
                        // In start tag.
                        i += 1;
                    }
                } else {
                    if (i == (startTag.length - angleBracket.length) && checkAttributes(b)) {
                        // Found start tag with attributes.
                        return true;
                    } else {
                        // Not in start tag.
                        i = 0;
                    }
                }
            }
        }
        // Unreachable./
//        return false;
    }

    public boolean readUntilEndElement() throws IOException {
        int si = 0;
        int ei = 0;
        int depth = 0;

        while (true) {
            int rb = raf.read();
            if (rb == -1) {
                // End of file (ignore end of split).
                return false;
            } else {
                databuffer.write(rb);
                int b = rb;
                if (b == startTag[si] && (b == endTag[ei] || checkEmptyTag(b, ei))) {
                    // In start tag or end tag.
                    si += 1;
                    ei += 1;
                } else if (b == startTag[si]) {
                    if (si >= startTag.length - 1) {
                        // Found start tag.
                        si = 0;
                        ei = 0;
                        depth += 1;
                    } else {
                        // In start tag.
                        si += 1;
                        ei = 0;
                    }
                } else if (b == endTag[ei] || checkEmptyTag(b, ei)) {
                    if ((b == endTag[ei] && ei >= endTag.length - 1) ||
                            (checkEmptyTag(b, ei) && ei >= endEmptyTag.length - 1)) {
                        if (depth == 0) {
                            // Found closing end tag.
                            return true;
                        } else {
                            // Found nested end tag.
                            si = 0;
                            ei = 0;
                            depth -= 1;
                        }
                    } else {
                        // In end tag.
                        si = 0;
                        ei += 1;
                    }
                } else {
                    // Not in start tag or end tag.
                    si = 0;
                    ei = 0;
                }
            }
        }
        // Unreachable.
    }

    public boolean checkEmptyTag(int currentLetter, int position) {
        if (position >= endEmptyTag.length) {
            return false;
        } else {
            return currentLetter == endEmptyTag[position];
        }
    }

    public boolean checkAttributes(int current) throws IOException {
        int len = 0;
        int b = current;
        while (len < space.length && b == space[len]) {
            len += 1;
            if (len >= space.length) {
                currentStartTag = subBytes(startTag, 0, startTag.length - angleBracket.length, space);

                return true;
            }
            b = raf.read();
        }
        return false;

    }

    public static byte[] subBytes(byte[] src, int begin, int count, byte[] space) {
        byte[] bs = new byte[count + 1];
        System.arraycopy(src, begin, bs, 0, count);
        bs[bs.length - 1] = space[0];
        return bs;
    }

    /*------------------XML分割线----------结束-------------------------*/


    public TailFile(File file, Map<String, String> headers, long inode, long pos, String parentDir)
            throws IOException {
        this.raf = new RandomAccessFile(file, "r");

        if (pos > 0) {
            raf.seek(pos);
            lineReadPos = pos;
        }
        this.path = file.getAbsolutePath();
        this.inode = inode;
        this.pos = pos;
        this.lastUpdated = 0L;
        this.needTail = true;
        this.headers = headers;
        this.oldBuffer = new byte[0];
        this.bufferPos = NEED_READING;

        this.fileName = file.getName();
        //XMl
        this.start = raf.getFilePointer();
        this.end = start + raf.length();
        this.filePath = file;
        this.parentDir = parentDir;
        //DBF
        this.fis = new FileInputStream(file);
        if (fileName.substring(fileName.length() - 4).toLowerCase().equals(".dbf")) {
            this.reader = new DBFReader(fis);
        }

    }

    public RandomAccessFile getRaf() {
        return raf;
    }

    public String getPath() {
        return path;
    }

    public long getInode() {
        return inode;
    }

    public long getPos() {
        return pos;
    }

    public long getLastUpdated() {
        return lastUpdated;
    }

    public boolean needTail() {
        return needTail;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public long getLineReadPos() {
        return lineReadPos;
    }

    public void setPos(long pos) {
        this.pos = pos;
    }

    public void setLastUpdated(long lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    public void setNeedTail(boolean needTail) {
        this.needTail = needTail;
    }

    public void setLineReadPos(long lineReadPos) {
        this.lineReadPos = lineReadPos;
    }

    //更新指针位置
    public boolean updatePos(String path, long inode, long pos) throws IOException {
        if (this.inode == inode && this.path.equals(path)) {

            setPos(pos);
            updateFilePos(pos);
            logger.info("Updated position, file: " + path + ", inode: " + inode + ", pos: " + pos);
            return true;
        }
        return false;
    }

    //更新文件指针位置
    public void updateFilePos(long pos) throws IOException {
        raf.seek(pos);
        lineReadPos = pos;
        bufferPos = NEED_READING;
        oldBuffer = new byte[0];
    }

    /**
     * @param numEvents 读取的事件数, backoffWithoutNL, addByteOffset 添加字节偏移量
     * @return java.util.List<org.apache.flume.Event>
     * @author wangshuai
     * @date 2018/9/14 16:35
     * @description
     */
    public List<Event> readEvents(int numEvents, boolean backoffWithoutNL,
                                  boolean addByteOffset) throws IOException {

        List<Event> events = Lists.newLinkedList();
        for (int i = 0; i < numEvents; i++) {
            Event event = readEvent(backoffWithoutNL, addByteOffset);
            if (event == null) {
                break;
            }
            events.add(event);
        }
        return events;
    }

    private Event readEvent(boolean backoffWithoutNL, boolean addByteOffset) throws IOException {
        Long posTmp = getLineReadPos();
        updateFilePos(posTmp);


        HashMap<String, String> map = new HashMap<>();
        String replace = filePath.getAbsolutePath().replace(parentDir, "");
        if (replace.length() > 4) {
            map.put("fileName", replace.substring(0, replace.length() - 4));
        } else {
            map.put("fileName", replace);
        }
        if (fileName.substring(fileName.length() - 4).toLowerCase().equals(".xml")) {
            String xmlNext = xmlNext();
            if (xmlNext != null) {
                JSONArray jsonArray = new JSONArray("[" + xmlNext + "]");
                String csv = CDL.toString(jsonArray);
                if (xmlRow == 0) {
                    Event event = EventBuilder.withBody(csv.substring(0, csv.length() - 1), Charset.forName("utf-8"));
                    event.setHeaders(map);
                    xmlRow++;
                    setLineReadPos(raf.getFilePointer());
                    return event;
                } else {
                    Event event = EventBuilder.withBody(csv.split("\n")[1], Charset.forName("utf-8"));
                    setLineReadPos(raf.getFilePointer());
                    event.setHeaders(map);
                    return event;
                }
            } else {
                return null;
            }
        } else if (fileName.substring(fileName.length() - 4).toLowerCase().equals(".dbf")) {
            Object[] rowValues = reader.nextRecord();
            if (rowValues != null && rowValues.length > 0) {
                JSONObject jsonObject = new JSONObject();
                for (int i = 0; i < rowValues.length; i++) {
                    if (rowValues[i] != null) {
                        jsonObject.put(reader.getField(i).getName(), new String(rowValues[i].toString().getBytes(Charset.forName("8859_1")), Charset.forName("GBK")));
                    } else {
                        setLineReadPos(filePath.length());
                    }
                }
                JSONArray jsonArray = new JSONArray("[" + jsonObject.toString() + "]");
                if (!jsonArray.isEmpty()) {
                    String csv = CDL.toString(jsonArray);
                    setLineReadPos(dbfRow);
                    if (dbfRow == 0) {
                        Event event = EventBuilder.withBody(csv.substring(0, csv.length() - 1), Charset.forName("utf-8"));
                        event.setHeaders(map);
                        dbfRow++;
                        return event;
                    } else {
                        Event event = EventBuilder.withBody(csv.split("\n")[1], Charset.forName("utf-8"));
                        event.setHeaders(map);
                        dbfRow++;
                        return event;
                    }
                } else {
                    setLineReadPos(filePath.length());
                    return null;
                }


            } else {
                setLineReadPos(filePath.length());
                return null;
            }
        } else if (fileName.substring(fileName.length() - 4).toLowerCase().equals(".tsv")) {
            LineResult line = readLine();
            if (line == null) {
                return null;
            }
            if (backoffWithoutNL && !line.lineSepInclude) {
                logger.info("Backing off in file without newline: "
                        + path + ", inode: " + inode + ", pos: " + raf.getFilePointer());
                updateFilePos(posTmp);
                return null;
            }
            String str = new String(line.line, Charset.forName("utf-8")).replace("\t", ",");
            Event event = EventBuilder.withBody(str.getBytes());
            if (addByteOffset == true) {
                event.getHeaders().put(BYTE_OFFSET_HEADER_KEY, posTmp.toString());
            }
            event.setHeaders(map);

            return event;

        }
        /*------------------TailFile----开始-----------------------*/

        LineResult line = readLine();

        if (line == null) {
            return null;
        }
        if (backoffWithoutNL && !line.lineSepInclude) {
            logger.info("Backing off in file without newline: "
                    + path + ", inode: " + inode + ", pos: " + raf.getFilePointer());
            updateFilePos(posTmp);
            return null;
        }
        Event event = EventBuilder.withBody(line.line);
        if (addByteOffset == true) {
            event.getHeaders().put(BYTE_OFFSET_HEADER_KEY, posTmp.toString());
        }
        event.setHeaders(map);
        /*------------------TailFile----结束-----------------------*/
        return event;
    }

    private void readFile() throws IOException {
        if ((raf.length() - raf.getFilePointer()) < BUFFER_SIZE) {
            //读取最后字节(不满足8192个字节时)
            buffer = new byte[(int) (raf.length() - raf.getFilePointer())];
        } else {
            buffer = new byte[BUFFER_SIZE];
        }
        raf.read(buffer, 0, buffer.length);
        bufferPos = 0;
    }

    private byte[] concatByteArrays(byte[] a, int startIdxA, int lenA,
                                    byte[] b, int startIdxB, int lenB) {
        byte[] c = new byte[lenA + lenB];
        System.arraycopy(a, startIdxA, c, 0, lenA);
        System.arraycopy(b, startIdxB, c, lenA, lenB);
        return c;
    }

    //读行
    public LineResult readLine() throws IOException {
        LineResult lineResult = null;
        while (true) {
            if (bufferPos == NEED_READING) {
                //判断指针当前位置小与文件的总长度
                if (raf.getFilePointer() < raf.length()) {
                    //调用读文件方法
                    readFile();
                } else {
                    if (oldBuffer.length > 0) {
                        lineResult = new LineResult(false, oldBuffer);
                        oldBuffer = new byte[0];
                        setLineReadPos(lineReadPos + lineResult.line.length);
                    }
                    break;
                }
            }
            for (int i = bufferPos; i < buffer.length; i++) {
                if (buffer[i] == BYTE_NL) {
                    int oldLen = oldBuffer.length;
                    // Don't copy last byte(NEW_LINE)
                    //一行的字节数
                    int lineLen = i - bufferPos;
                    // For windows, check for CR
                    if (i > 0 && buffer[i - 1] == BYTE_CR) {
                        lineLen -= 1;
                    } else if (oldBuffer.length > 0 && oldBuffer[oldBuffer.length - 1] == BYTE_CR) {
                        oldLen -= 1;
                    }
                    lineResult = new LineResult(true,
                            concatByteArrays(oldBuffer, 0, oldLen, buffer, bufferPos, lineLen));
                    setLineReadPos(lineReadPos + (oldBuffer.length + (i - bufferPos + 1)));
                    oldBuffer = new byte[0];
                    if (i + 1 < buffer.length) {
                        bufferPos = i + 1;
                    } else {
                        bufferPos = NEED_READING;
                    }
                    break;
                }
            }
            if (lineResult != null) {
                break;
            }
            // NEW_LINE not showed up at the end of the buffer
            oldBuffer = concatByteArrays(oldBuffer, 0, oldBuffer.length,
                    buffer, bufferPos, buffer.length - bufferPos);
            bufferPos = NEED_READING;
        }
        return lineResult;
    }

    public void close() {
        try {
            raf.close();
            raf = null;
            long now = System.currentTimeMillis();
            setLastUpdated(now);
            String path = filePath.toString();
//            if (!path.endsWith(".xlsd")) {
//                filePath.renameTo(new File(path + now + ".xlsd"));
//            }
        } catch (IOException e) {
            logger.error("Failed closing file: " + path + ", inode: " + inode, e);
        }
    }

    private class LineResult {
        final boolean lineSepInclude;
        final byte[] line;

        public LineResult(boolean lineSepInclude, byte[] line) {
            super();
            this.lineSepInclude = lineSepInclude;
            this.line = line;
        }
    }


    public static class CompareByLastModifiedTime implements Comparator<File> {
        @Override
        public int compare(File f1, File f2) {
            return Long.valueOf(f1.lastModified()).compareTo(f2.lastModified());
        }
    }

}
