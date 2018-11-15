package com.yss.scala.util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

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
    val DATEKEY_FORMAT = new SimpleDateFormat(YYYYMMDD)
    val d1 = DATEKEY_FORMAT.parse(date1).getTime
    val d2 = DATEKEY_FORMAT.parse(date2).getTime
    Math.abs(d1 - d2) / (24 * 60 * 60 * 1000)
  }


  /**
    * 在原来的日期基础上增加指定的天数
    *
    * @param dateStr 格式为yyyyMMdd eg:20180528
    * @param days    天数
    * @param format  日期格式
    * @return 指定日期格式为字符串
    */
  def addDays(dateStr: String, days: Int, format: String = YYYYMMDD): String = {
    val DATEKEY_FORMAT = new SimpleDateFormat(format)
    val date = DATEKEY_FORMAT.parse(dateStr)
    val calender = Calendar.getInstance()
    calender.setTime(date)
    calender.add(Calendar.DAY_OF_YEAR, days)
    val resDate = DATEKEY_FORMAT.format(calender.getTime)
    calender.clear()
    resDate
  }


  /** 日期格式：yyyyMMdd */
  val YYYYMMDD = "yyyyMMdd"

  /** 日期格式：yyyy-MM-dd HH:mm:ss */
  val YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss"

  val YYYY_MM_DD = "yyyy-MM-dd"

  /** 获取当天的日期 */
  def getToday(pattern: String) = {
    val sdf = new SimpleDateFormat(pattern)
    sdf.format(new Date())
  }

  /**
    * 格式化时间
    *
    * @param dateLong :时间戳
    * @return 返回格式:格式为yyyyMMdd
    */
  def formatDate(dateLong: Long, pattern: String = YYYYMMDD): String = {
    val date = new Date(dateLong)
    val DATEKEY_FORMAT = new SimpleDateFormat(pattern)
    DATEKEY_FORMAT.format(date)
  }

  /**
    * 将格式化的时间转化成时间戳
    *
    * @param formattedDate 格式化的时间:eg:2018-01-01 00:00:00
    * @param pattern       时间格式
    * @return
    */
  def formattedDate2Long(formattedDate: String, pattern: String = YYYY_MM_DD): Long = {
    new SimpleDateFormat(pattern).parse(formattedDate).getTime
  }


  /**
    * 将日期的格式进行转换
    * @param orgDate 原始的日期 如 20180209
    * @param orgPattern 原始的日期格式 如 YYYYMMMDD
    * @param newPattern 需要转化成得日期格式 如 YYYY-MM-DD
    * @return
    */
  def changeDateForm(orgDate: String, orgPattern: String, newPattern: String): String = {
    formatDate(formattedDate2Long(orgDate, orgPattern), newPattern)
  }
}
