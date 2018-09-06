package com.yss.flumesink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author yupan
 * 2018-08-28 16:01
 */
public class MySinks extends AbstractSink implements Configurable {
    private static final Logger logger = LoggerFactory.getLogger(MySinks.class);
    private static final String PROP_KEY_ROOTPATH = "fileName";
    private String fileName;
    private Configuration conf;
    private String time;
    private String comparisontime;


    private SimpleDateFormat df;



    @Override
    public Status process() throws EventDeliveryException {
        Status status =null;
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        Event event =null;
        txn.begin();
        while(true){
            event =ch.take();
            if(event !=null){
                break;
            }
        }
        try {
            logger.debug("Get event.");

//            System.out.println("event.getBody()-----" + body+"::::"+body.length());
//            String res =body + ":" + System.currentTimeMillis() + "\r\n";
//            File file = new File(fileName);
//            FileOutputStream fos = null;
//            try {
//                fos = new FileOutputStream(file, true);
//            } catch (FileNotFoundException e) {
//                e.printStackTrace();
//            }
//            try {
//                System.out.println("写入数据"+res);
//                fos.write(res.getBytes());
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            try {
//                fos.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }


            String[] values=event.getHeaders().get("file").split("/");
            String lasttime = event.getHeaders().get("lasttime");
            String filename = values[values.length-1];
            System.out.println("文件名称："+filename);
            Path path =new Path("hdfs://192.168.235.4:9000/flume/"+time+"/"+filename);;

//
            FileSystem fs = FileSystem.get(conf);
            FSDataOutputStream foshadoop=null;
            if( fs.exists(path) && lasttime.equalsIgnoreCase(comparisontime)){
                foshadoop = fs.append(path);
            }else if (fs.exists(path) && !lasttime.equalsIgnoreCase(comparisontime)){
                fs.delete(path,true);
                comparisontime=lasttime;
                foshadoop = fs.create(path, true);
            }else {
                foshadoop = fs.create(path, true);
            }
            foshadoop.write(event.getBody());
//            if (path.toString().endsWith("dbf")){
//                foshadoop.write(event.getBody());
//            }
//            if(path.toString().endsWith("tsv")|| path.toString().endsWith("txt") || path.toString().endsWith("csv") ) {
//                foshadoop.write(addBytes(event.getBody(), "\r\n".getBytes()));
//            }
            foshadoop.close();
            fs.close();


            status=Status.READY;
            txn.commit();
        }catch (Throwable th){
            txn.rollback();
            status=Status.BACKOFF;
            if ( th instanceof Error){
                throw (Error)th;
            }else{
                throw new EventDeliveryException(th);
            }
        }finally {
            txn.close();
        }
        return status;
    }



    @Override
    public void configure(Context context) {

        fileName= context.getString(PROP_KEY_ROOTPATH);
        conf = new Configuration();
        Date date = new Date();
        df = new SimpleDateFormat("yyyy-MM-dd");
        time=df.format(date);
        comparisontime="0";

    }

    @Override
    public synchronized void start() {
        super.start();
    }

    @Override
    public synchronized void stop() {
        super.stop();
    }


    public static byte[] addBytes(byte[] data1, byte[] data2) {
        byte[] data3 = new byte[data1.length + data2.length];
        System.arraycopy(data1, 0, data3, 0, data1.length);
        System.arraycopy(data2, 0, data3, data1.length, data2.length);
        return data3;
    }

    }
