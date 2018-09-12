package com.yss.kafka

import java.util

import org.apache.hadoop.hbase.client.{Admin, Connection, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import com.yss.kafka.Common._


object ToHbase{

  def sendHbase (message : String , conn : Connection) = {
    val admin = conn.getAdmin
    val tablename = TableName.valueOf(hbaseTableName)
    val family = new HColumnDescriptor(hbaseFamilyName)
    if(admin.tableExists(tablename)){
      putResult(conn,admin,tablename,message,family)
    }else{
      createTable(admin,tablename,family)
      putResult(conn,admin,tablename,message,family)
    }
  }

  def dateToDate (date : String) : String = {
//    var timeStamp = ""
//    val simpleDateFormat = new SimpleDateFormat(timePattern)
//    val dates = simpleDateFormat.parse(date)
//    val ts = dates.getTime
//    timeStamp = String.valueOf(sts)
//    timeStamp
    val dates = date.split("-")
    val year = dates(0)
    val month = dates(1)
    val day = dates(2)
    year+month+day
  }

  def createTable (admin : Admin , tablename : TableName , family : HColumnDescriptor) = {
    val tableDesc = new HTableDescriptor(tablename)
    family.setMaxVersions(3)
    tableDesc.addFamily(family)
    admin.createTable(tableDesc)
  }

  def putResult (conn : Connection , admin : Admin , tableName: TableName , message : String , family : HColumnDescriptor) = {
    if(admin.tableExists(tableName))
      if(admin.isTableDisabled(tableName)){
        admin.enableTable(tableName)
      }
    val messageSplit = message.split(",")
    val ID    = messageSplit(0).trim.toString
    val FGDDM = messageSplit(1).trim.toString
    val FGDXM = messageSplit(2).trim.toString
    val FSZSH = messageSplit(3).trim.toString
    val FSH   = messageSplit(4).trim.toString
    val FZZR  = messageSplit(5).trim.toString
    val FSETCODE = messageSplit(6).trim.toString
    val FCHK     = messageSplit(7).trim.toString
    val FSTARTDATE   = messageSplit(8).trim.toString
    val FACCOUNTTYPT = messageSplit(9).trim.toString

    val rowkey = ID + "_" + dateToDate(FSTARTDATE) + "_" + FGDDM

    val put0 = new Put(rowkey.getBytes())
    put0.addColumn(family.getName,Bytes.toBytes("ID"),ID.getBytes())
    val put1 = new Put(rowkey.getBytes())
    put1.addColumn(family.getName,Bytes.toBytes("FGDDM"),FGDDM.getBytes())
    val put2 = new Put(rowkey.getBytes())
    put2.addColumn(family.getName,Bytes.toBytes("FGDXM"),FGDXM.getBytes())
    val put3 = new Put(rowkey.getBytes())
    put3.addColumn(family.getName,Bytes.toBytes("FSZSH"),FSZSH.getBytes())
    val put4 = new Put(rowkey.getBytes())
    put4.addColumn(family.getName,Bytes.toBytes("FSH"),FSH.getBytes())
    val put5 = new Put(rowkey.getBytes())
    put5.addColumn(family.getName,Bytes.toBytes("FZZR"),FZZR.getBytes())
    val put6 = new Put(rowkey.getBytes())
    put6.addColumn(family.getName,Bytes.toBytes("FSETCODE"),FSETCODE.getBytes())
    val put7 = new Put(rowkey.getBytes())
    put7.addColumn(family.getName,Bytes.toBytes("FCHK"),FCHK.getBytes())
    val put8 = new Put(rowkey.getBytes())
    put8.addColumn(family.getName,Bytes.toBytes("FSTARTDATE"),FSTARTDATE.getBytes())
    val put9 = new Put(rowkey.getBytes())
    put9.addColumn(family.getName,Bytes.toBytes("FACCOUNTTYPT"),FACCOUNTTYPT.getBytes())
    var list  = new util.ArrayList[Put]()
    list.add(put0)
    list.add(put1)
    list.add(put2)
    list.add(put3)
    list.add(put4)
    list.add(put5)
    list.add(put6)
    list.add(put7)
    list.add(put8)
    list.add(put9)
    val table = conn.getTable(tableName)
    table.put(list)
    table.close()
  }

}
