package com.yss.source.utils;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.hadoop.io.DataOutputBuffer;
import org.json.CDL;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.XML;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.time.LocalDateTime;

/**
 * @author wangshuai
 * @version 2018-09-14 17:11
 * describe:
 * 目标文件：
 * 目标表：
 */
public class ReadXml {


    private int eventLines;
    private StringBuffer bodyBuffer = new StringBuffer();
    private String csvSeparator;
    private String startLabel;
    private String currentRecord;
    private byte[] startTag;
    private byte[] currentStartTag;
    private byte[] endTag;
    private byte[] endEmptyTag = "/>".getBytes(Charset.defaultCharset());
    private byte[] space = " ".getBytes(Charset.defaultCharset());
    private byte[] angleBracket = ">".getBytes(Charset.defaultCharset());
    private Long start;
    private long ROW = 1;
    private DataOutputBuffer databuffer = new DataOutputBuffer();
    private RandomAccessFile raf;

    public ReadXml(String startLabel, RandomAccessFile raf, long startPos, String currentRecord, String csvSeparator, int eventLines) {
        this.startTag = ("<" + startLabel + ">").getBytes(Charset.forName("utf-8"));
        this.endTag = ("</" + startLabel + ">").getBytes(Charset.forName("utf-8"));
        this.raf = raf;
        this.start = startPos;
        this.startLabel = startLabel;
        this.currentRecord = currentRecord;
        this.csvSeparator = csvSeparator;
        this.eventLines = eventLines;
    }

    public String read() throws IOException {
        if (readUntilStartElement()) {
            try {
                databuffer.write(currentStartTag);
                if (readUntilEndElement()) {
                    JSONObject nObject = XML.toJSONObject(new String(databuffer.getData(), 0, databuffer.getLength()));
                    String data = nObject.get(startLabel).toString();
                    if (data != null) {
                        JSONArray jsonArray = new JSONArray("[" + data + "]");
                        String csv = CDL.toString(jsonArray);
                        if (!csvSeparator.equals(",")) {
                            csv = csv.replaceAll(",", csvSeparator);
                        }
                        return csv;
                    } else {
                        return null;
                    }
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
            if (b == -1) {
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


    public Event readNode() throws IOException {

        if (ROW == 1) {
            String xmlNext = read();
            if (xmlNext != null) {
                Event event = EventBuilder.withBody(xmlNext.split("\n")[0], Charset.forName("utf-8"));
                event.getHeaders().put(currentRecord, String.valueOf(ROW));
                ROW++;
                raf.seek(start);
                return event;
            } else {
                System.out.println(LocalDateTime.now() + "    XMl空白文件!");
                return null;
            }
        } else {
            for (int a = 0; a < 10; a++) {
                String data = read();
                if (data != null) {
                    bodyBuffer.append(data.split("\n")[1]);
                    bodyBuffer.append("\n");
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

