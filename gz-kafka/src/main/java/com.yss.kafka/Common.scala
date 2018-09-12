package com.yss.kafka

object Common {

  /*
  Spark
   */

  val inputPathPattern = "yyyyMMdd"
  val appName = "KafkaProducerSpark"
  val sqlTable = "gudong"
  val sqlText = "select row_number() OVER (ORDER BY FGDXM) as ID,* from gudong"

  /*
  Kafka
   */

  val kafkaTopic = "sixtopic"

  /*
  Kafka配置信息
   */

  val zkConnect  = "bj-rack001-hadoop002:2181,bj-rack001-hadoop003:2181,bj-rack001-hadoop004:2181"
  val brokerList = "bj-rack001-hadoop006:6667"
  val serializableClass = "kafka.serializer.StringEncoder"
  val groupId = "kafkatopic"

  /*
  Hbase配置信息
   */

  val zkQuorum = "bj-rack001-hadoop002,bj-rack001-hadoop003,bj-rack001-hadoop004"
  val zkClientPort = "2181"
  val zkParent = "/hbase-unsecure"
  val hbaseTableName = "CSGDZH"
  val hbaseFamilyName = "family"
  val timePattern = "yyyy-MM-dd"

}
