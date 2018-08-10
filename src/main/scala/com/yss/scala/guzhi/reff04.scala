package com.yss.scala.guzhi

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
  * @author yupan
  *         2018/8/6 10:04
  **/
object reff04 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("reff04").getOrCreate()

    val sc = spark.sparkContext

    val reff04_input = "C:\\Users\\yupan\\Desktop\\核算\\接口文件\\reff040704.txt"
//    val reff04_input = "C:\\Users\\yupan\\Desktop\\reff040704.txt"
    val reff04_output = "C:\\Users\\yupan\\Desktop\\test"
//    val reff04_data = sc.textFile(reff04_input)
    val reff04_data = sc.hadoopFile(reff04_input, classOf[TextInputFormat], classOf[LongWritable], classOf[Text]).map(f => new String(f._2.getBytes,0,f._2.getLength,"GBK"))


    val reff04_final = reff04_data.map(f=>{
      val value_split=f.split("\\|")
      val RFStreamID = "R0401"                                             //参考数据类型
      val valSecurityID = securityIDBu(value_split(1).trim)                 //证券代码
      val ZhSecurityID =  zhSecurityIDBu(valSecurityID)                     //转换后系统识别的六位代码
      val ISIN = value_split(2).trim                                        //ISIN代码
      val Symbol = value_split(3).trim                                      //中文证券简称
      val SymbolEn = value_split(4).trim                                    //英文证券简称
      val SecurityDesc = value_split(5).trim                               //英文全称
      val UnderlyingSecurityID = value_split(6).trim                        //辅助证券代码
      val MarketID =   value_split(7).trim                                  //市场种类
      val SecurityType =  value_split(8).trim                               //证券类别
      val Currency =   value_split(9).trim                                  //货币种类
      val AmountTimes =   value_split(10).trim                              //货币单位
      val PerValue =      value_split(11).trim                              //面值
      val PerValueCurrency =   value_split(12).trim                         //面值货币种类
      val Interest =     value_split(13).trim                               //利息
      val IssueDate =    value_split(14).trim                               //上市日期
      val RoundLot =     value_split(15).trim                               //买卖单位
      val PreClosePx =    value_split(16).trim                              //前收盘价格
      val Text =         value_split(17).trim                               //备注
      val SecurityStatusFlag =    value_split(18).trim                      //产品状态信息
      val value = RFStreamID+","+
                  valSecurityID+","+
                  ZhSecurityID+","+
                  ISIN+","+
                  Symbol+","+
                  SymbolEn+","+
                  SecurityDesc+","+
                  UnderlyingSecurityID+","+
                  MarketID+","+
                  SecurityType+","+
                  Currency+","+
                  AmountTimes+","+
                  PerValue+","+
                  PerValueCurrency+","+
                  Interest+","+
                  IssueDate+","+
                  RoundLot+","+
                  PreClosePx+","+
                  Text+","+
                  SecurityStatusFlag
      value
    })

    val path = new Path(reff04_output)
    val conf = new Configuration()
    path.getFileSystem(conf).delete(path,true)
    reff04_final.saveAsTextFile(path.toString)

    val name ="RFStreamID,valSecurityID,ZhSecurityID,ISIN,Symbol,SymbolEn,SecurityDesc,UnderlyingSecurityID,MarketID,SecurityType,Currency,AmountTimes,PerValue,PerValueCurrency,Interest,IssueDate,RoundLot,PreClosePx,Text,SecurityStatusFlag"
    val field =name.split(",").map(f=>StructField(f,StringType,nullable = true))
    val schema = StructType(field)

    val rowRDD = reff04_final
      .map(_.split(","))
      .map(attributes =>  Row(attributes(0).trim,
        attributes(1).trim,
        attributes(2).trim,
        attributes(3).trim,
        attributes(4).trim,
        attributes(5).trim,
        attributes(6).trim,
        attributes(7).trim,
        attributes(8).trim,
        attributes(9).trim,
        attributes(10).trim,
        attributes(11).trim,
        attributes(12).trim,
        attributes(13).trim,
        attributes(14).trim,
        attributes(15).trim,
        attributes(16).trim,
        attributes(17).trim,
        attributes(18).trim,
        attributes(19).trim))


    val reff04_dataframe = spark.createDataFrame(rowRDD, schema)
    val url ="jdbc:mysql://192.168.102.119:3306/JJCWGZ?useUnicode=true&characterEncoding=UTF-8"
    val table = "reff04"
    val prop = new Properties()
    prop.setProperty("user","test01")
    prop.setProperty("password","test01")

    reff04_dataframe.write.mode(SaveMode.Append).jdbc(url,table,prop)




    //    reff04_final.foreachPartition(insert)
  }

  def securityIDBu(SecurityID:String):String={
    if(SecurityID.length>=5){
      SecurityID
    }else{
      val len = 5-SecurityID.length
      "0"*len+SecurityID
    }
  }
  def zhSecurityIDBu(ZhSecurityID:String):String={
    if(ZhSecurityID.length>=6){
      ZhSecurityID
    }else{
      "H"+ZhSecurityID
    }
  }

  def insert(iterator: Iterator[String]): Unit = {

    var conn: Connection = null

    var ps: PreparedStatement = null

    val database = "test"

    val user = "root"

    val password = "Yssbigdata01@"

    val conn_str = "jdbc:mysql://192.168.235.4:3306/"+database+"?user="+user+"&password="+password

    val sql = "INSERT INTO reff04(RFStreamID,valSecurityID,ZhSecurityID,ISIN,Symbol,SymbolEn,SecurityDesc,UnderlyingSecurityID,MarketID,SecurityType,Currency,AmountTimes,PerValue,PerValueCurrency,Interest,IssueDate,RoundLot,PreClosePx,Text,SecurityStatusFlag) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

    try {

      conn = DriverManager.getConnection(conn_str)

      iterator.foreach(data => {

        val f= data.split(",")
        ps = conn.prepareStatement(sql)

        ps.setString(1, f(0))
        ps.setString(2, f(1))
        ps.setString(3, f(2))
        ps.setString(4, f(3))
        ps.setString(5, f(4))
        ps.setString(6, f(5))
        ps.setString(7, f(6))
        ps.setString(8, f(7))
        ps.setString(9, f(8))
        ps.setString(10, f(9))
        ps.setString(11, f(10))
        ps.setString(12, f(11))
        ps.setString(13, f(12))
        ps.setString(14, f(13))
        ps.setString(15, f(14))
        ps.setString(16, f(15))
        ps.setString(17, f(16))
        ps.setString(18, f(17))
        ps.setString(19, f(18))
        ps.setString(20, f(19))
        ps.executeUpdate()

      })

    } catch {

      case e: Exception => println(e)

    } finally {

      if (ps != null) {

        ps.close()

      }

      if (conn != null) {

        conn.close()

      }

    }

  }

}
