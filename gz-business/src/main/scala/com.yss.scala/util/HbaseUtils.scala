package com.yss.scala.util

import java.util.Calendar
import java.util.concurrent.Executors

import com.yss.scala.dto.Hzjkqs
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * @author yupan
  * @version 2018-11-08 10:04
  *          describe: 
  *          目标文件：
  *          目标表：
  */

class HbaseUtils {



  /**
    * 关闭连接
    * @param conn
    */
  def closeConn(conn:Connection):Unit={
    if(conn != null){
    conn.close
    }
  }

  /**
    * 设置hbase的参数，主要是zookeeper地址、端口以及hbase在zookeeper中存储的节点
    * @return
    */

  def getconf(): Configuration ={
    val conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum","192.168.102.121")

    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    conf
  }


  /**
    *
    * @param conf         hbase设置的参数
    * @param tablename   表名
    * @return        mapreducer运行时设置的属性
    */

  def getJobConf(conf:Configuration,tablename:String):JobConf={
    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,tablename)
    jobConf
  }

  /**
    *
    * @return  获取hbase连接
    */

  def getConn(conf:Configuration):Connection={
    val pool = Executors.newFixedThreadPool(100)
    val conn = ConnectionFactory.createConnection(conf,pool)
    conn
  }

  /**
    * @param conn    Hbase 连接
    * @param tablename   表名
    * @param columncluster  列簇
    * @param rowkey    rowkey
    * @param columnname  列名
    * @return  Hbase get方法获取的值
    */

  def getHbaseData(conn:Connection,tablename:String,columncluster:String,rowkey:String,columnname:String): String = {
    val table = conn.getTable(TableName.valueOf(tablename))
    val get = new Get(Bytes.toBytes(rowkey))
    val result = table.get(get)

    val value = result.getValue(Bytes.toBytes(columncluster),Bytes.toBytes(columnname))
    table.close()
    val valuefinal=Bytes.toString(value)
    valuefinal
  }

  /**
    *默认的列簇名称为"cf"
    * @param conn Hbase连接
    * @param tablename   表名
    * @param rowkey       rowkey
    * @param columnname   列名
    * @return         Hbase get方法获取列名对应的值
    */
  def getHbaseDataDefaultColumnCluster(conn:Connection,tablename:String,rowkey:String,columnname:String): String = {

    val table = conn.getTable(TableName.valueOf(tablename))
    val get = new Get(Bytes.toBytes(rowkey))
    val result = table.get(get)

    val value = result.getValue(Bytes.toBytes("cf"),Bytes.toBytes(columnname))
    table.close()
    val valuefinal=Bytes.toString(value)
    valuefinal
  }

  /**
    *默认的列簇为"cf"
    * @param conn
    * @param tablename  表名
    * @param rowkey  rowkey
    * @return  hbase get方法获取rowkey对应的所有列的值  是一个Map结构
    */

  def getHbaseAllDataDefaultColumnCluster(conn:Connection,tablename:String,rowkey:String): Map[String,String] = {
    val resultarray=new mutable.HashMap[String,String]()

    val table = conn.getTable(TableName.valueOf(tablename))
    val get = new Get(Bytes.toBytes(rowkey))
    val result = table.get(get)
    table.close()

    for(cell<-result.rawCells()){
      val value = Bytes.toString(CellUtil.cloneValue(cell));
//      val key = CellUtil.getCellKeyAsString(cell)
      val key = Bytes.toString(cell.getQualifier)
      resultarray.put(key,value)
    }
    resultarray.toMap
  }

  /**
    * 读取目录下所有filename的文件（匹配日期文件）
    * @param conf   配置参数
    * @param sparkSession
    * @param tablename    表名
    * @param fieldName    表字段信息
    * @param filename     文件名称
    */

  def writeDataToHZJKQS(conf:Configuration,sparkSession: SparkSession,tablename: String,fieldName:Array[String],filename:String):Unit={

    val jobConf =getJobConf(conf,tablename)

    val indataRDD = sparkSession.sparkContext.textFile("C:\\Users\\dell\\Desktop\\test\\[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]\\data1\\"+filename)
    val rdd = indataRDD.map(_.split(",")).map(f=>{
      val put = new Put(Bytes.toBytes(f(0).toInt))
      for(i <- 0 to f.length){
        put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes(fieldName(i)),Bytes.toBytes(f(0)))
      }
      (new ImmutableBytesWritable,put)
    })
    rdd.saveAsHadoopDataset(jobConf)

  }

  /**
    * 读取目录下某个固定月份的下的filename文件
    * @param conf   配置参数
    * @param sparkSession
    * @param tablename  表名
    * @param fieldName  字段信息
    * @param filename   文件名称
    * @param month      月份
    */
  def writeMonDataToHZJKQS(conf:Configuration,sparkSession: SparkSession,tablename: String,fieldName:Array[String],filename:String,month:String):Unit={

    val jobConf =getJobConf(conf,tablename)

    val date = Calendar.getInstance();
    val year = String.valueOf(date.get(Calendar.YEAR));

    val indataRDD = sparkSession.sparkContext.textFile("C:\\Users\\dell\\Desktop\\test\\"+year+month+"[0-9][0-9]\\data1\\"+filename)
    val rdd = indataRDD.map(_.split(",")).map(f=>{
      val put = new Put(Bytes.toBytes(f(0).toInt))
      for(i <- 0 to f.length){
        put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes(fieldName(i)),Bytes.toBytes(f(0)))
      }
      (new ImmutableBytesWritable,put)
    })
    rdd.saveAsHadoopDataset(jobConf)
  }

  /**
    * 读取固定目录下的filename文件
    * @param conf   配置参数
    * @param sparkSession
    * @param tablename
    * @param fieldName
    * @param filename
    * @param date
    */
  def writeSomeDateDataToHZJKQS(conf:Configuration,sparkSession: SparkSession,tablename: String,fieldName:Array[String],filename:String,date:String):Unit={
    val jobConf =getJobConf(conf,tablename)

    val indataRDD = sparkSession.sparkContext.textFile("C:\\Users\\dell\\Desktop\\test\\"+date+"\\data1\\"+filename)
    val rdd = indataRDD.map(_.split(",")).map(f=>{
      val put = new Put(Bytes.toBytes(f(0).toInt))
      for(i <- 0 to f.length){
        put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes(fieldName(i)),Bytes.toBytes(f(0)))
      }
      (new ImmutableBytesWritable,put)
    })
    rdd.saveAsHadoopDataset(jobConf)
  }
}


object HbaseUtils{
  private val hbaseUtils =new HbaseUtils

  def getInstance=hbaseUtils
}
