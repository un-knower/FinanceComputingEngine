package com.yss.test.scala

import java.util.Properties

import com.yss.scala.util.BasicUtils

object Test1 {
  def main(args: Array[String]): Unit = {
    val str1 = "Hello"
    val str2 = "World"

     var pro =new Properties();
    //    pro.load(this.getClass.getResourceAsStream("/basic.properties"))
    //println(ShZQChange.contactString("@", str1, str2,"haha"))
//    println(DateUtils.changeDateForm("20180209",DateUtils.YYYYMMDD,DateUtils.YYYY_MM_DD))

//    pro.load(this.getClass.getResourceAsStream("/basic.properties"))
//    val namenodePath = pro.getProperty("namenode_path")
//    val gzInterfaceDir = pro.getProperty("gz_interfacedir")
//    val gzOutputDir = pro.getProperty("gz_outputdir")
//    val gzBasicList = pro.getProperty("gz_basic_list")
//    val user = pro.getProperty("user")
//    val password = pro.getProperty("password")
    for (a <- 1 to 10)
      println(BasicUtils.properties)
//    val driver = pro.getProperty("driver")
//    val jdbc = pro.getProperty("jdbc")
//    val masterType = pro.getProperty("master_type")
//    val properties = pro



  }
}
