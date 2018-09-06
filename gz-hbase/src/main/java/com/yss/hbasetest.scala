package com.yss

import org.apache.hadoop.hbase.client.{HBaseAdmin, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor}
import org.apache.hadoop.mapred.JobConf

/**
  * @author yupan
  *         2018/8/17 10:32
  **/
object hbasetest {
 def main(args:Array[String]):Unit={
   val spark = SparkSession.builder().master("local[*]").appName("yss").getOrCreate()
   val sc = spark.sparkContext

   val conf = HBaseConfiguration.create()
   conf.set("hbase.zookeeper.quorum","192.168.235.4:2181")


   val tablename ="tt"


   val admin = new HBaseAdmin(conf)
   if(!admin.tableExists(tablename)){
     val table = new HTableDescriptor(Bytes.toBytes(tablename))
     val family = new HColumnDescriptor(Bytes.toBytes("cf"))
     table.addFamily(family)
     admin.createTable(table)
   }

   val jobConf = new JobConf(conf)
   jobConf.setOutputFormat(classOf[TableOutputFormat])
   jobConf.set(TableOutputFormat.OUTPUT_TABLE,tablename)


   val indataRDD = sc.makeRDD(Array("1,jack,15","2,Lily,16","3,余攀,16"))
   val rdd = indataRDD.map(_.split(",")).map(f=>{
     val put = new Put(Bytes.toBytes(f(0).toInt))
     put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("name"),Bytes.toBytes(f(1)))
     put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("age"),Bytes.toBytes(f(2).toInt))
     (new ImmutableBytesWritable,put)
   })
    rdd.saveAsHadoopDataset(jobConf)
    sc.stop()
 }
}
