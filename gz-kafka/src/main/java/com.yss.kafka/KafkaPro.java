package com.yss.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Producer;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;
import java.util.concurrent.Future;

/***
 * kafka 生产数据
 */


public class KafkaPro {

    public static void main(String[] args){
        //192.168.102.115:9092
        Properties props = new Properties();//get an properties
        props.put("zookeeper.connect", "192.168.102.120:2181,192.168.102.121:2181,192.168.102.122:2181");
        props.put("serializer.class", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("metadata.broker.list", "192.168.102.120:6667");
        props.put("bootstrap.servers", "192.168.102.120:6667");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
       // properties.put("metadata.broker.list", "192.168.102.120:9092,192.168.102.121:9092,192.168.102.122:9092");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        try {
            BufferedReader reader = new BufferedReader(new FileReader("E:\\DBFFILE\\InterDoc\\09000211trddata20180420.txt"));
            String value = reader.readLine();
//            String value = null;
            int i =1;
            String topic = "newtopic";
            while(value!=null){
                ProducerRecord<String, String> message = new ProducerRecord<String, String>(topic,i+"@"+value);
               if(!value.isEmpty()) {
                   producer.send(message);
               }
                System.out.println("The first line is :"+i+"@"+value);
                i++;
                value = reader.readLine();
                //System.out.println(value);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}

