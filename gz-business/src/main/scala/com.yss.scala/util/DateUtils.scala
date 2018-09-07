package com.yss.scala.util

import java.text.SimpleDateFormat
import java.util.Calendar

/**
  * @auther: lijiayan
  * @date: 2018/9/6
  * @desc: 日期操作工具类
  */
object DateUtils {

  /**
    * 计算两个日期相隔天数
    *
    * @param date1 格式为yyyyMMdd eg:20180528
    * @param date2 格式为yyyyMMdd eg:20180528
    * @return
    */
  def absDays(date1: String, date2: String): Long = {
    val DATEKEY_FORMAT = new SimpleDateFormat("yyyyMMdd")
    val d1 = DATEKEY_FORMAT.parse(date1).getTime
    val d2 = DATEKEY_FORMAT.parse(date2).getTime
    Math.abs(d1 - d2) / (24 * 60 * 60 * 1000)
  }


  /**
    * 在原来的日期基础上增加指定的天数
    *
    * @param dateStr 格式为yyyyMMdd eg:20180528
    * @param days    天数
    * @return 格式为yyyyMMdd
    */
  def addDays(dateStr: String, days: Int): String = {
    val DATEKEY_FORMAT = new SimpleDateFormat("yyyyMMdd")
    val date = DATEKEY_FORMAT.parse(dateStr)
    val calender = Calendar.getInstance()
    calender.setTime(date)
    calender.add(Calendar.DAY_OF_YEAR, days)
    val resDate = DATEKEY_FORMAT.format(calender.getTime)
    calender.clear()
    resDate
  }

}
