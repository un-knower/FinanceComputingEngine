package com.yss.java.dbf;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.text.DecimalFormat;

public class DBFField implements Serializable
{

    public static final int TERMINATOR = 0x0d;
    public static DecimalFormat decimalFormat = new DecimalFormat("00");

    public String fieldName;                   /* 0-10  */
    public byte dataType;                      /* 11    */
    private int reserved1;                     /* 12-15 */
    public int fieldLength;                    /* 16    */
    public byte decimalCount;                  /* 17    */
    private short reserved2;                   /* 18-19 */
    private byte workAreaId;                   /* 20    */
    private short reserved3;                   /* 21-22 */
    private byte setFieldsFlag;                /* 23    */
    private byte[] reserved4 = new byte[7];    /* 24-30 */
    private byte indexFieldFlag;               /* 31    */

    private DBFField()
    {
    }

    public static DBFField read(final DataInput in) throws IOException
    {
        final DBFField field = new DBFField();

        final byte firstByte = in.readByte();                     /* 0     */
        if (firstByte == TERMINATOR)
        {
            return null;
        }

        final byte[] bytes = new byte[11];                      /* 1-10  */
        in.readFully(bytes, 1, 10);
        bytes[0] = firstByte;

        int nonZeroIndex = bytes.length - 1;
        while (nonZeroIndex >= 0 && bytes[nonZeroIndex] == 0)
        {
            nonZeroIndex--;
        }
        field.fieldName = new String(bytes, 0, nonZeroIndex + 1);

        field.dataType = in.readByte();                     /* 11    */
        field.reserved1 = in.readInt();// DbfUtils.readLittleEndianInt(in);   /* 12-15 */
        field.fieldLength = in.readUnsignedByte();          /* 16    */
        field.decimalCount = in.readByte();                 /* 17    */
        field.reserved2 = in.readShort(); // DbfUtils.readLittleEndianShort(in); /* 18-19 */
        field.workAreaId = in.readByte();                   /* 20    */
        field.reserved3 = in.readShort();// DbfUtils.readLittleEndianShort(in); /* 21-22 */
        field.setFieldsFlag = in.readByte();                /* 23    */
        in.readFully(field.reserved4);                        /* 24-30 */
        field.indexFieldFlag = in.readByte();               /* 31    */

        return field;
    }

    public Object readValue(final DataInputStream dataInputStream) throws IOException
    {
        final byte bytes[] = new byte[fieldLength];
        dataInputStream.readFully(bytes);
        return new String(bytes).trim();
    }

    public Writable readWritable(final DataInputStream dataInputStream) throws IOException
    {
        final byte bytes[] = new byte[fieldLength];
        dataInputStream.readFully(bytes);
        return new Text(new String(bytes).trim());
    }

    private int parseInt(
            final byte[] bytes,
            final int from,
            final int to)
    {
        int result = 0;
        for (int i = from; i < to && i < bytes.length; i++)
        {
            result *= 10;
            result += bytes[i] - '0';
        }
        return result;
    }

    private short parseShort(
            final byte[] bytes,
            final int from,
            final int to)
    {
        short result = 0;
        for (int i = from; i < to && i < bytes.length; i++)
        {
            result *= 10;
            result += bytes[i] - '0';
        }
        return result;
    }

    private long parseLong(
            final byte[] bytes,
            final int from,
            final int to)
    {
        long result = 0L;
        for (int i = from; i < to && i < bytes.length; i++)
        {
            result *= 10L;
            result += bytes[i] - '0';
        }
        return result;
    }

    private int trimSpaces(final byte[] bytes)
    {
        int i = 0, l = bytes.length;
        while (i < l)
        {
            if (bytes[i] != ' ')
            {
                break;
            }
            i++;
        }
        return i;
    }

    private String readTimeInMillis(final byte[] bytes) throws IOException
    {
        int year = parseInt(bytes, 0, 4);
        int month = parseInt(bytes, 4, 6);
        int day = parseInt(bytes, 6, 8);
        return year+ decimalFormat.format(month)+decimalFormat.format(day);
    }

    private boolean readLogical(final byte[] bytes) throws IOException
    {
        return bytes[0] == 'Y' || bytes[0] == 'y' || bytes[0] == 'T' || bytes[0] == 't';
    }

    private short readShort(final byte[] bytes) throws IOException
    {
        final int index = trimSpaces(bytes);
        final int length = bytes.length - index;
        if (length == 0 || bytes[index] == '?')
        {
            return 0;
        }
        return parseShort(bytes, index, bytes.length);
    }

    private int readInteger(final byte[] bytes) throws IOException
    {
        final int index = trimSpaces(bytes);
        final int length = bytes.length - index;
        if (length == 0 || bytes[index] == '?')
        {
            return 0;
        }
        return parseInt(bytes, index, bytes.length);
    }

    private long readLong(final byte[] bytes) throws IOException
    {
        final int index = trimSpaces(bytes);
        final int length = bytes.length - index;
        if (length == 0 || bytes[index] == '?')
        {
            return 0L;
        }
        return parseLong(bytes, index, bytes.length);
    }

    private float readFloat(final byte[] bytes) throws IOException
    {
        final int index = trimSpaces(bytes);
        final int length = bytes.length - index;
        if (length == 0 || bytes[index] == '?')
        {
            return 0.0F;
        }
        // TODO - inline float reader
        return Float.parseFloat(new String(bytes, index, length));
    }

    private double readDouble(final byte[] bytes) throws IOException
    {
        final int index = trimSpaces(bytes);
        final int length = bytes.length - index;
        if (length == 0 || bytes[index] == '?')
        {
            return 0.0;
        }
        // TODO - inline double reader
        return Double.parseDouble(new String(bytes, index, length));
    }

    @Override
    public String toString()
    {
        final char c = (char) dataType;
        final StringBuilder sb = new StringBuilder("DBFField{");
        sb.append("fieldName='")
          .append(fieldName)
          .append('\'');
        sb.append(", dataType='")
          .append(c)
          .append('\'');
        sb.append(", fieldLength=")
          .append(fieldLength);
        sb.append(", decimalCount=")
          .append(decimalCount);
        sb.append('}');
        return sb.toString();
    }
}