package com.yss.source.utils;


import com.linuxense.javadbf.DBFException;
import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFReader;
import com.linuxense.javadbf.DBFUtils;

import java.io.*;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

/**
 * @author wangshuai
 * @version 2018-11-02 11:27
 * describe:
 * 目标文件：
 * 目标表：
 */


public class DBFReadUtil extends DBFReader {

    private static final long MILLISECS_PER_DAY = 24 * 60 * 60 * 1000;
    private static final long TIME_MILLIS_1_1_4713_BC = -210866803200000L;
    private boolean trimRightSpaces = true;
    private DBFMemoFileUtil memoFile = null;

    public DBFReadUtil(InputStream in) {
        super(in);
    }

    public DBFReadUtil(InputStream in, Boolean showDeletedRows) {
        super(in, showDeletedRows);
    }

    public DBFReadUtil(InputStream in, Charset charset) {
        super(in, charset);
    }

    public DBFReadUtil(InputStream in, Charset charset, boolean showDeletedRows) {
        super(in, charset, showDeletedRows);
    }

    @Override
    protected Object getFieldValue(DBFField field) throws IOException {
        int bytesReaded = 0;
        switch (field.getType()) {
            case CHARACTER:
                byte b_array[] = new byte[field.getLength()];
                bytesReaded = this.dataInputStream.read(b_array);
                if (bytesReaded < field.getLength()) {
                    throw new EOFException("Unexpected end of file");
                }
                if (this.trimRightSpaces) {
                    return new String(DBFUtils.trimRightSpaces(b_array), getCharset());
                } else {
                    return new String(b_array, getCharset());
                }

            case VARCHAR:
            case VARBINARY:
                byte b_array_var[] = new byte[field.getLength()];
                bytesReaded = this.dataInputStream.read(b_array_var);
                if (bytesReaded < field.getLength()) {
                    throw new EOFException("Unexpected end of file");
                }
                return b_array_var;
            case DATE:

                byte t_byte_year[] = new byte[4];
                bytesReaded = this.dataInputStream.read(t_byte_year);
                if (bytesReaded < 4) {
                    throw new EOFException("Unexpected end of file");
                }

                byte t_byte_month[] = new byte[2];
                bytesReaded = this.dataInputStream.read(t_byte_month);
                if (bytesReaded < 2) {
                    throw new EOFException("Unexpected end of file");
                }

                byte t_byte_day[] = new byte[2];
                bytesReaded = this.dataInputStream.read(t_byte_day);
                if (bytesReaded < 2) {
                    throw new EOFException("Unexpected end of file");
                }

                try {
                    GregorianCalendar calendar = new GregorianCalendar(Integer.parseInt(new String(t_byte_year, StandardCharsets.US_ASCII)),
                            Integer.parseInt(new String(t_byte_month, StandardCharsets.US_ASCII)) - 1,
                            Integer.parseInt(new String(t_byte_day, StandardCharsets.US_ASCII)));
                    return calendar.getTime();
                } catch (NumberFormatException e) {
                    // this field may be empty or may have improper value set
                    return null;
                }


            case FLOATING_POINT:
            case NUMERIC:
//                return DBFUtils.readNumericStoredAsText(this.dataInputStream, field.getLength());
                return readNumericStoredAsText(this.dataInputStream, field.getLength());
            case LOGICAL:
                byte t_logical = this.dataInputStream.readByte();
                return DBFUtils.toBoolean(t_logical);
            case LONG:
            case AUTOINCREMENT:
                int data = DBFUtils.readLittleEndianInt(this.dataInputStream);
                return data;
            case CURRENCY:
                int c_data = DBFUtils.readLittleEndianInt(this.dataInputStream);
                String s_data = String.format("%05d", c_data);
                String x1 = s_data.substring(0, s_data.length() - 4);
                String x2 = s_data.substring(s_data.length() - 4);

                skip(field.getLength() - 4);
                return new BigDecimal(x1 + "." + x2);
            case TIMESTAMP:
            case TIMESTAMP_DBASE7:
                int days = DBFUtils.readLittleEndianInt(this.dataInputStream);
                int time = DBFUtils.readLittleEndianInt(this.dataInputStream);

                if (days == 0 && time == 0) {
                    return null;
                } else {
                    Calendar calendar = new GregorianCalendar();
                    calendar.setTimeInMillis(days * MILLISECS_PER_DAY + TIME_MILLIS_1_1_4713_BC + time);
                    calendar.add(Calendar.MILLISECOND, -TimeZone.getDefault().getOffset(calendar.getTimeInMillis()));
                    return calendar.getTime();
                }
            case MEMO:
            case GENERAL_OLE:
            case PICTURE:
            case BLOB:
                return readMemoField(field);
            case BINARY:
                if (field.getLength() == 8) {
                    return readDoubleField(field);
                } else {
                    return readMemoField(field);
                }
            case DOUBLE:
                return readDoubleField(field);
            case NULL_FLAGS:
                byte[] data1 = new byte[field.getLength()];
                int readed = dataInputStream.read(data1);
                if (readed != field.getLength()) {
                    throw new EOFException("Unexpected end of file");
                }
                return BitSet.valueOf(data1);
            default:
                skip(field.getLength());
                return null;
        }
    }


    @Override
    public void setMemoFile(File memoFile) {
        if (this.memoFile != null) {
            throw new IllegalStateException("Memo file is already setted");
        }
        if (!memoFile.exists()) {
            throw new DBFException("Memo file " + memoFile.getName() + " not exists");
        }
        if (!memoFile.canRead()) {
            throw new DBFException("Cannot read Memo file " + memoFile.getName());
        }
        this.memoFile = new DBFMemoFileUtil(memoFile, this.getCharset());
    }

    private Object readDoubleField(DBFField field) throws IOException {
        byte[] data = new byte[field.getLength()];
        int bytesReaded = this.dataInputStream.read(data);
        if (bytesReaded < field.getLength()) {
            throw new EOFException("Unexpected end of file");
        }
        return ByteBuffer.wrap(
                new byte[]{
                        data[7], data[6], data[5], data[4],
                        data[3], data[2], data[1], data[0]
                }).getDouble();
    }


    private Object readMemoField(DBFField field) throws IOException {
        Number nBlock = null;
        if (field.getLength() == 10) {
            nBlock = DBFUtils.readNumericStoredAsText(this.dataInputStream, field.getLength());
        } else {
            nBlock = DBFUtils.readLittleEndianInt(this.dataInputStream);
        }
        if (this.memoFile != null && nBlock != null) {
            return memoFile.readData(nBlock.intValue(), field.getType());
        }
        return null;
    }

    public static String readNumericStoredAsText(DataInputStream dataInput, int length) throws IOException {
        try {
            byte t_float[] = new byte[length];
            int readed = dataInput.read(t_float);
            if (readed != length) {
                throw new EOFException("failed to read:" + length + " bytes");
            }
            t_float = DBFUtils.removeSpaces(t_float);
            if (t_float.length > 0 && DBFUtils.isPureAscii(t_float) && !DBFUtils.contains(t_float, (byte) '?') && !DBFUtils.contains(t_float, (byte) '*')) {
                String aux = new String(t_float, StandardCharsets.US_ASCII).replace(',', '.');
                if (".".equals(aux)) {
                    return BigDecimal.ZERO.toString();
                }else if (aux.startsWith(".")) {
                    return 0 + aux;
                }
//                return new BigDecimal(aux);
                return aux;
            } else {
                return null;
            }
        } catch (NumberFormatException e) {
            throw new DBFException("Failed to parse Float: " + e.getMessage(), e);
        }
    }
}
