package com.yss.kafka

import java.util.Properties

import kafka.serializer.StringDecoder
import kafka.utils.VerifiableProperties
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import com.yss.kafka.Common._
import com.yss.kafka.ToHbase._
import kafka.consumer.ConsumerConfig

/**
  * @author wfy
  * @version 2018-09-10
  *          描述：KAFKA消费数据
  */



object KafkaConsumer{

  def main(args: Array[String]): Unit = {
    //HBASE配置信息
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", zkClientPort)
    conf.set("hbase.zookeeper.quorum",zkQuorum)
    conf.set("zookeeper.znode.parent", zkParent)
    val conn = ConnectionFactory.createConnection(conf)
    //Consumer配置信息
    val prop = new Properties
    prop.put("zookeeper.connect" , zkConnect)
    prop.put("group.id" , groupId)
    val config = new ConsumerConfig(prop)
    val createConsumer = kafka.consumer.Consumer.create(config)
    val topic = Map(kafkaTopic -> 1)
    val keyDecoder = new StringDecoder(new VerifiableProperties())
    val valueDecoder = new StringDecoder(new VerifiableProperties())
    val streams = createConsumer.createMessageStreams(topic,keyDecoder,valueDecoder)
    val results = streams.get(kafkaTopic).get
    for (result <- results){
      val iterator = result.iterator()
      while(iterator.hasNext()){
        val message = iterator.next().message()
        sendHbase(message,conn)
      }
    }
  }

}
