package com.yss.scala.util

import org.apache.spark.sql.Row

/**
  * @auther: lijiayan
  * @date: 2018/11/5
  * @desc: 用于对 {@link org.apache.spark.sql.Row} 取数据时的操作工具类
  */
object RowUtils {

  /**
    * 根据row中的字段获取字段对应的值
    * @param row  org.apache.spark.sql.Row
    * @param fieldName 字段名
    * @param defaultValue 默认值
    * @return
    */
   def getRowFieldAsString(row: Row, fieldName: String, defaultValue: String = ""): String = {
    var field = row.getAs[String](fieldName)
    if (field == null) {
      field = defaultValue
    } else {
      field = field.trim
    }
    field
  }
}
