package com.yss.test.scala

import com.yss.scala.util.DateUtils

object Test1 {
  def main(args: Array[String]): Unit = {
     val date = "1900-01-01"
    println(DateUtils.formattedDate2Long(date,DateUtils.YYYY_MM_DD))
  }
}
