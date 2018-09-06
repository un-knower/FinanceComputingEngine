package com.yss

import org.apache.hadoop.hbase.client.{HBaseAdmin, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}

/**
  * @author yupan
  *         2018/8/17 14:45
  **/
object ReadHbase {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("yss").getOrCreate()
    val sc = spark.sparkContext

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum","192.168.235.4:2181")


    val tablename="tt"
    conf.set(TableInputFormat.INPUT_TABLE, tablename)
    val admin = new HBaseAdmin(conf)
    if(!admin.isTableAvailable(tablename)){
      val table = new HTableDescriptor(TableName.valueOf(tablename))
      val family = new HColumnDescriptor(Bytes.toBytes("cf"))
      table.addFamily(family)
      admin.createTable(table)
    }

    val hbaseRdd = sc.newAPIHadoopRDD(conf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    val count = hbaseRdd.count()
    println(count)
    hbaseRdd.foreach{case (_,result)=>{
      val key = Bytes.toInt(result.getRow)
      val name = Bytes.toString(result.getValue("cf".getBytes,"name".getBytes()))
      val age = Bytes.toInt(result.getValue("cf".getBytes,"age".getBytes))
      println("Row key:"+key+" Name:"+name+" Age:"+age)
    }}
  }
}
