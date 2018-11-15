package com.yss.test.scala

import com.yss.scala.util.DateUtils

object Test1 {
  def main(args: Array[String]): Unit = {
    val str1 = "Hello"
    val str2 = "World"
    //println(ShZQChange.contactString("@", str1, str2,"haha"))
    println(DateUtils.changeDateForm("20180209",DateUtils.YYYYMMDD,DateUtils.YYYY_MM_DD))
  }
}
