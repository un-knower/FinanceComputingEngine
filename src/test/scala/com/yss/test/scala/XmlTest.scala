package com.yss.test.scala

import org.apache.spark.sql.SparkSession

/**
  * @author yupan
  *         2018/8/2 15:04
  **/
object XmlTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
//    val customSchema = StructType(Array(
//      StructField("_id", StringType, nullable = true),
//      StructField("author", StringType, nullable = true),
//      StructField("description", StringType, nullable = true),
//      StructField("genre", StringType ,nullable = true),
//      StructField("price", DoubleType, nullable = true),
//      StructField("publish_date", StringType, nullable = true),
//      StructField("title", StringType, nullable = true)))

    val sc = spark.sparkContext

    val df = spark.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "Security")
//      .schema(customSchema)
//      .load("C:\\Users\\yupan\\Desktop\\imcexchangerate.xml")
      .load("C:\\Users\\yupan\\Desktop\\imcexchangerate.xml")


//    val pcsdf = df.select("author", "_id")

//    df.rdd.saveAsTextFile("C:\\Users\\yupan\\Desktop\\xxData")
//    val selectedData = df.select("author", "_id")
//    selectedData.write
//      .format("com.databricks.spark.xml")
//      .option("rootTag", "books")
//      .option("rowTag", "book")
//      .save("C:\\Users\\yupan\\Desktop\\mmm.xml")
    df.show()
  }
}
