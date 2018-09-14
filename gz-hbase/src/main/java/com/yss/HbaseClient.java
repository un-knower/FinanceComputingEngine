package com.yss;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * created by 张锴
 *
 */
public class HbaseClient {

    private static final String TABLE_NAME = "TestTable";
    private static final String ROWKEY = "299977#20180420#09000211#01250566#S#8385c03c";
    private static final String COULMNFAMILIY = "QHList";
    private static final String COULMN = "SXF";

    private static String stampToDate(Long s) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long lt = new Long(s);
        Date date = new Date(lt);
        String res = simpleDateFormat.format(date);
        return res;
    }

    /**
     * scan 全表
     * @throws IOException
     */
    private void queryAll() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, TABLE_NAME);
        Scan s = new Scan();
        ResultScanner rs = table.getScanner(s);
        Long starttime = System.currentTimeMillis();
        SimpleDateFormat s1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        for (Result r : rs) {
            for (Cell cell : r.rawCells()) {
                System.out.println(
                        "Rowkey : " + Bytes.toString(r.getRow()) +
                                "   Familiy:QHList : " + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                                "   Value : " + Bytes.toString(CellUtil.cloneValue(cell)) +
                                "   Time : " + cell.getTimestamp()
                );
            }
        }
        Long stoptime = System.currentTimeMillis();
        try {
            Date date1 = s1.parse(stampToDate(starttime));
            Date date2 = s1.parse(stampToDate(stoptime));
            Long diff = (date2.getTime() - date1.getTime()) / 1000;
            System.out.println("总共花费：" + diff + "秒");

        } catch (ParseException e) {
            e.printStackTrace();
        }
        table.close();

    }

    /**
     * 根据表名和rowkey查找一条数据
     *
     * @throws IOException
     */
    private String  getOne(String table,String rowkey) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        HTable t = new HTable(conf, table);
        Get get = new Get(Bytes.toBytes(rowkey));
        Result r = t.get(get);
        String result =" ";

            for (Cell cell : r.rawCells()) {
                result=Bytes.toString(r.getRow()) +"    "+
                    Bytes.toString(CellUtil.cloneFamily(cell))+"    "+
                    Bytes.toString(CellUtil.cloneQualifier(cell))+"     "+
                    Bytes.toString(CellUtil.cloneValue(cell))+"     "+result;
        }
        t.close();
        return result;

    }

    /**
     * 根据表名，rowkey，列族，列名查找列的值
     *
     * @throws IOException
     */
    private String getColumn(String tableName, String rowkey, String family, String column) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, TABLE_NAME);
        Get get = new Get(Bytes.toBytes(ROWKEY));
        get.addColumn(Bytes.toBytes(COULMNFAMILIY), Bytes.toBytes(COULMN));
        Result r = table.get(get);
        String result ="";
        for (Cell cell : r.rawCells()) {
            result= Bytes.toString(r.getRow()) +"   "+
                    Bytes.toString(CellUtil.cloneFamily(cell))+"    "+
                    Bytes.toString(CellUtil.cloneQualifier(cell)) +"    "+
                    Bytes.toString(CellUtil.cloneValue(cell))+result;

        }
        table.close();
        return result;
    }



    public static void main(String[] args) throws Exception {

        HbaseClient client = new HbaseClient();

        String s=client.getColumn(TABLE_NAME,ROWKEY,COULMNFAMILIY,COULMN);
        System.out.println(s);

    }
}
