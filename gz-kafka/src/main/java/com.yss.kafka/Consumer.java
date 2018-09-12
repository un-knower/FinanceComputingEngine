package com.yss.kafka;

import kafka.utils.ShutdownableThread;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.*;

public class Consumer extends ShutdownableThread {
    private final KafkaConsumer<Integer, String> consumer;
    private final String topic;
    int i = 0;
    static Configuration conf = null;

    static {
        conf = HBaseConfiguration.create();
    }

    public Consumer(String topic) {
        super("KafkaConsumerExample", false);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "hz");
//        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
//        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
//        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<>(props);
        this.topic = topic;
    }

    @Override
    public void doWork() {
        consumer.subscribe(Collections.singletonList(this.topic));
        ConsumerRecords<Integer, String> records = consumer.poll(1000);

       // System.out.println(records.count());

        String tableName = "TestTable";
        String columnFamily = "QHList";
        String[] column = new String[0];
        HTable table = null;
        try {
            HBaseAdmin admin = new HBaseAdmin(conf);
            if (!admin.tableExists(Bytes.toBytes(tableName))) {
                System.err.println("the table " + tableName + " is not exist");
                System.exit(1);
            }
            admin.close();
            column = new String[]{"XH", "DATE", "ZJZH", "CJLS", "PZHY", "MS", "CJL", "CJJ", "CJE", "CJSJ", "KPC", "TJTB", "PCYKR", "PCYKB", "SXF",
                    "JYBM", "TYBS", "JYHY", "BDH", "XWH", "BZ", "CJRQ"};
            table = new HTable(conf, TableName.valueOf(tableName));
            //将数据自动提交功能关闭
//            table.setAutoFlushTo(true);

            //设置数据缓存区域
            //table.setWriteBufferSize(128 * 1024);
        } catch (IOException e) {
            e.printStackTrace();
        }
        List<Put> lists = new ArrayList<>();
        // HTable table = new HTable(conf, tableName);
        for (ConsumerRecord<Integer, String> record : records) {
            System.out.println("Received message: " + record.value());
            String str[] = record.value().split("@");
            String xh = str[0];
            String date = str[1].replace("-", "");
            String zjzh = str[2];
            String cjls = str[3];
            String ms = str[5];
            String xwh = str[19];
            String rowkey = xh + "#" + date + "#" + zjzh + "#" + cjls + "#" + ms + "#" + xwh;
            Put put = new Put(Bytes.toBytes(rowkey));
            for (int j = 0; j <= 21; j++) {
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column[j]), Bytes.toBytes(record.value().split("@")[j]));
            }
            lists.add(put);
            i++;
            System.out.println(i + "--------------"+rowkey);

        }
        try {
            table.put(lists);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public boolean isInterruptible() {
        return false;
    }

    public static void main(String[] args) {
//        boolean isAsync = args.length == 0 || !args[0].trim().equalsIgnoreCase("sync");
//        Producer producerThread = new Producer(KafkaProperties.TOPIC, isAsync);
//        producerThread.start();

        Consumer consumerThread = new Consumer(KafkaProperties.TOPIC);
        consumerThread.start();

    }
}
