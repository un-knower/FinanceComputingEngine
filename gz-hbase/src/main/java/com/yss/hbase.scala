package com.yss

/**
  * @author yupan
  *         2018/8/17 13:55
  **/
object hbase {
  def main(args: Array[String]): Unit = {

    val conf= HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "192.168.235.4")
    conf.set("hbase.zookeeper.property.clientPort", "2181")

    val conn = ConnectionFactory.createConnection(conf)
    val table = conn.getTable(TableName.valueOf("hbase1"))
    val put = new Put(Bytes.toBytes("row1"))
    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("id"), Bytes.toBytes(1))
    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes("tom"))
    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes(15))
    table.put(put)
    table.close

  }
}
