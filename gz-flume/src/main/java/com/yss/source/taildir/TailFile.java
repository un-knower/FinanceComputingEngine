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

package com.yss.source.taildir;

import com.google.common.collect.Lists;
import com.yss.source.utils.ReadDbf;
import com.yss.source.utils.ReadXml;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public class TailFile {
    private static final Logger logger = LoggerFactory.getLogger(TailFile.class);

    private static final byte BYTE_NL = (byte) 10;
    private static final byte BYTE_CR = (byte) 13;

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

    /*-------------------------*/
    private int endRow = 0;
    private ReadDbf readDbf;
    private ReadXml readXml;
    private FileInputStream fileInputStream;

    private String fileSuffixes;
    private String currentRecord;
    private String parentDir;
    private final String csvSeparator;
    private final Boolean renameFlie;

    /*-------------------------*/


    public TailFile(File file, Map<String, String> headers, long inode, long pos, String parentDir,
                    String xmlNode, String currentRecord, String csvSeparator, boolean renameFlie, Integer eventLines)
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
        /*---------------------------------------------------------------*/
        this.fileInputStream = new FileInputStream(file);
        this.parentDir = parentDir;
        this.renameFlie = renameFlie;
        this.csvSeparator = csvSeparator;
        this.currentRecord = currentRecord;
        String fileName = file.getName();
        fileSuffixes = fileName.substring(fileName.length() - 4);
        System.out.println(LocalDateTime.now()+"    Tail创建文件的对象准备开始读取文件:" + file.getAbsolutePath());
        if (fileSuffixes.equalsIgnoreCase(".dbf")) {
            readDbf = new ReadDbf(this.fileInputStream, currentRecord, csvSeparator, eventLines);
        } else if (fileSuffixes.equalsIgnoreCase(".xml")) {
            readXml = new ReadXml(xmlNode, this.raf, this.pos, currentRecord, csvSeparator, eventLines);
        }
        /*---------------------------------------------------------------*/
    }

    public RandomAccessFile getRaf() {
        return raf;
    }

    public String getParentDir() {
        return parentDir;
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

    public boolean updatePos(String path, long inode, long pos) throws IOException {
        if (this.inode == inode && this.path.equals(path)) {
            setPos(pos);
            updateFilePos(pos);
            logger.info("Updated position, file: " + path + ", inode: " + inode + ", pos: " + pos);
            return true;
        }
        return false;
    }

    public void updateFilePos(long pos) throws IOException {
        raf.seek(pos);
        lineReadPos = pos;
        bufferPos = NEED_READING;
        oldBuffer = new byte[0];
    }


    public List<Event> readEvents(int numEvents, boolean backoffWithoutNL,
                                  boolean addByteOffset) throws IOException {
        List<Event> events = Lists.newLinkedList();
        for (int i = 0; i < numEvents; i++) {
            Event event = readEvent(backoffWithoutNL, addByteOffset);
            if (event == null) {
                if (endRow == 0) {
                    System.out.println(LocalDateTime.now() + "    监控文件已读到最后,发送标识位!");
                    Event eventBody = EventBuilder.withBody("fileEnd".getBytes("utf-8"));
                    eventBody.getHeaders().put(currentRecord, String.valueOf(1));
                    events.add(eventBody);
                    endRow++;
                }
                break;
            }
            events.add(event);
        }
        return events;
    }

    private Event readEvent(boolean backoffWithoutNL, boolean addByteOffset) throws IOException {
        Event event;
        if (fileSuffixes.equalsIgnoreCase(".dbf")) {
            event = readDbf.readDBFFile();
            //更新pos,从而更新posJson文件
            setPos(fileInputStream.getChannel().position());

        } else if (fileSuffixes.equalsIgnoreCase(".xml")) {
            event = readXml.readNode();
            if (event == null) {
                setPos(raf.length());
            }
        } else {
            Long posTmp = getLineReadPos();
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
            if (fileSuffixes.equalsIgnoreCase(".tsv")) {
                event = EventBuilder.withBody(new String(line.line, Charset.forName("utf-8"))
                        .replaceAll("\t", csvSeparator).getBytes(Charset.forName("utf-8")));
            } else {
                event = EventBuilder.withBody(line.line);
                if (addByteOffset == true) {
                    event.getHeaders().put(TaildirSourceConfigurationConstants.BYTE_OFFSET_HEADER_KEY, posTmp.toString());
                }
            }

        }
        return event;
    }

    private void readFile() throws IOException {
        if ((raf.length() - raf.getFilePointer()) < BUFFER_SIZE) {
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

    public LineResult readLine() throws IOException {
        LineResult lineResult = null;
        while (true) {
            if (bufferPos == NEED_READING) {
                if (raf.getFilePointer() < raf.length()) {
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
            //读取完后对文件修改文件的后缀
            if (renameFlie) {
                File file = new File(getPath());
                if (!path.endsWith(".COMPLETED")) {
                    file.renameTo(new File(path + now + ".COMPLETED"));
                }
            }

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
}
