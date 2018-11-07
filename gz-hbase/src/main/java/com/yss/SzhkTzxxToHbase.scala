package com.yss

import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor}
import org.apache.hadoop.hbase.client.{HBaseAdmin, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SzhkTzxxToHbase {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("yss").getOrCreate()
    val sc = spark.sparkContext

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "192.168.235.4:2181")

    val tablename = "SzhkTzxx"

    val admin = new HBaseAdmin(conf)
    if (!admin.tableExists(tablename)) {
      val table = new HTableDescriptor(Bytes.toBytes(tablename))
      val family = new HColumnDescriptor(Bytes.toBytes("cf"))
      table.addFamily(family)
      admin.createTable(table)
    }

    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)

    val dataRDD = sc.textFile("")
    //val indataRDD = sc.makeRDD(Array("1,jack,15","2,Lily,16","3,余攀,16"))
    val rdd = dataRDD.flatMap(_.split("\t")).map(f => {
     // val list =List[String]()
      val scdm = f(0)
      val tzlb = f(1)
      val tzrq = f(2)
      val zqdm = f(3)
      val gfxz =f(4)
      val sszt =f(5)
      val qylb =f(6)
      val qybh =f(7)
      val zh1 =f(8)
      val zh2 =f(9)
      val rq1 =f(10)
      val rq2 =f(11)
      val rq3 =f(12)
      val je1= f(13)
      val je2 =f(14)
      val je3 =f(15)
      val jg1 =f(16)
      val jg2 =f(17)
      val bz =f(18)
      val hl=f(19)
      val bL1=f(20)
      val bL2 =f(21)
      val sL1=f(22)
      val sL2 =f(23)
      val LX1 =f(24)
      val LX2 =f(25)
      val fzdm1 =f(26)
      val fzdm2 =f(27)
      val fjsm1 =f(28)
      val fjsm2 =f(29)
      val fby =f(30)
      val zt =f(31)

      val s1 =scdm+"\t"+tzlb+"\t"+tzrq+"\t"+zqdm+"\t"+gfxz+"\t"+sszt+"\t"+qylb+"\t"+qybh+"\t"+zh1+"\t"+zh2+"\t"+rq1+"\t"+rq2+"\t"+rq3+"\t"
      +je1+"\t"+je2+"\t"+je3+"\t"+jg1+"\t"+jg2+"\t"+bz+"\t"+hl+"\t"+bL1+"\t"+bL2+"\t"+sL1+"\t"+sL2+"\t"+LX1+"\t"+LX2+"\t"+fjsm1+"\t"+fjsm2+"\t"+fby+"\t"+zt

      val s2 =scdm+"\t"+tzlb+"\t"+tzrq+"\t"+fzdm1+"\t"+gfxz+"\t"+sszt+"\t"+qylb+"\t"+qybh+"\t"+zh1+"\t"+zh2+"\t"+rq1+"\t"+rq2+"\t"+rq3+"\t"
      +je1+"\t"+je2+"\t"+je3+"\t"+jg1+"\t"+jg2+"\t"+bz+"\t"+hl+"\t"+bL1+"\t"+bL2+"\t"+sL1+"\t"+sL2+"\t"+LX1+"\t"+LX2+"\t"+fjsm1+"\t"+fjsm2+"\t"+fby+"\t"+zt

      val s3 =scdm+"\t"+tzlb+"\t"+tzrq+"\t"+fzdm2+"\t"+gfxz+"\t"+sszt+"\t"+qylb+"\t"+qybh+"\t"+zh1+"\t"+zh2+"\t"+rq1+"\t"+rq2+"\t"+rq3+"\t"
      +je1+"\t"+je2+"\t"+je3+"\t"+jg1+"\t"+jg2+"\t"+bz+"\t"+hl+"\t"+bL1+"\t"+bL2+"\t"+sL1+"\t"+sL2+"\t"+LX1+"\t"+LX2+"\t"+fjsm1+"\t"+fjsm2+"\t"+fby+"\t"+zt
      (s1,s2,s3)
    }).map(m => {
      val v = m._1.split("\t")

      val rowkey = v(1)+v(3)
      val put = new Put(Bytes.toBytes(rowkey))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("SCDM"), Bytes.toBytes(v(0)))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("TZLB"), Bytes.toBytes(v(1)))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("TZRQ"), Bytes.toBytes(v(2)))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("ZQDM"), Bytes.toBytes(v(3)))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("GFXZ"), Bytes.toBytes(v(4)))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("SSZT"), Bytes.toBytes(v(5)))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("QYLB"), Bytes.toBytes(v(6)))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("QYBH"), Bytes.toBytes(v(7)))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("ZH1"), Bytes.toBytes(v(8)))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("ZH2"), Bytes.toBytes(v(9)))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("RQ1"), Bytes.toBytes(v(10)))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("RQ2"), Bytes.toBytes(v(11)))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("RQ3"), Bytes.toBytes(v(12)))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("JE1"), Bytes.toBytes(v(13)))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("JE2"), Bytes.toBytes(v(14)))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("JE3"), Bytes.toBytes(v(15)))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("JG1"), Bytes.toBytes(v(16)))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("JG2"), Bytes.toBytes(v(17)))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("BZ"), Bytes.toBytes(v(18)))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("HL"), Bytes.toBytes(v(19)))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("BL1"), Bytes.toBytes(v(20)))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("BL2"), Bytes.toBytes(v(21)))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("SL1"), Bytes.toBytes(v(22)))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("SL2"), Bytes.toBytes(v(23)))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("LX1"), Bytes.toBytes(v(24)))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("LX2"), Bytes.toBytes(v(25)))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("FJSM1"), Bytes.toBytes(v(26)))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("FJSM2"), Bytes.toBytes(v(27)))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("FBY"), Bytes.toBytes(v(28)))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("ZT"), Bytes.toBytes(v(29)))



      (new ImmutableBytesWritable, put)
    })
    rdd.saveAsHadoopDataset(jobConf)
    sc.stop()
  }
}
