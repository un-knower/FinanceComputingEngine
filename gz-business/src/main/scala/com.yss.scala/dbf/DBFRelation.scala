package com.yss.scala.dbf


import java.io.FileNotFoundException

import com.yss.java.dbf.{DBFField, DBFHeader}
import com.yss.java.maprd.DBFInputFormat
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io._
import org.apache.spark.sql._
import org.apache.spark.sql.sources.{BaseRelation, PrunedScan}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._
import scala.util.control.Breaks

/**
  * Extends `PrunedScan` to map the RDD to specified columns.
  *
  * @param location   the location of the dbf file
  * @param sqlContext the SQL context
  *
  *                   TODO - extend PrunedFilterScan to apply 'smart' filter !
  */
case class DBFRelation(location: String)(@transient val sqlContext: SQLContext) extends BaseRelation with PrunedScan {

  /**
    * http://polyglot-window.blogspot.com/2009/03/arm-blocks-in-scala-revisited.html
    * http://stackoverflow.com/questions/2207425/what-automatic-resource-management-alternatives-exists-for-scala
    */
  private[dbf] def using[A <: {def close() : Unit}, B](r: A)(f: A => B): B = try {
    f(r)
  }
  finally {
    r.close()
  }

  private[dbf] def toStructField(field: DBFField): StructField = {
    StructField(field.fieldName, toDataType(field))
  }

  private[dbf] def toDataType(field: DBFField): DataType = {
    StringType
  }

  private[dbf] def toNumeType(field: DBFField): DataType = {
    //    println(field.fieldLength)
    if (field.decimalCount > 0) DoubleType
    else if (field.fieldLength < 3) ShortType
    else if (field.fieldLength < 5) IntegerType
    else if (field.fieldLength < 8) IntegerType else LongType
  }

  /**
    * Determine the RDD Schema based on the DBF header info.
    *
    * @return StructType instance
    */
  override def schema = {
    var path = new Path(location)
    val fs = FileSystem.get(path.toUri, sqlContext.sparkContext.hadoopConfiguration)
    val iterator = fs.listFiles(path, true)
    var realPath: Path = null
    val loop = new Breaks
    loop.breakable {
      while (iterator.hasNext) {
        val childPath = iterator.next().getPath
        if (!childPath.getName.startsWith("_")) {
          realPath = childPath
          loop.break
        }
      }
    }
    if (realPath == null) throw new FileNotFoundException("文件不存在：" + path)
    using(fs.open(realPath)) { dataInputStream => {
      StructType(DBFHeader.read(dataInputStream).fields.asScala.map(toStructField(_)))
    }
    }
  }

  /**
    * This is depending on Hadoop's implementation of InputFormat in the Shapefile project
    *
    * TODO - Optimize in Shapefile lib to return List(values) rather than Map(field name -> value)
    */
  private[dbf] val baseRDD = sqlContext.sparkContext.hadoopFile(
    location,
    classOf[DBFInputFormat],
    classOf[LongWritable],
    classOf[MapWritable],
    sqlContext.sparkContext.defaultMinPartitions
  ) // TODO - should this be cached ?

  private[dbf] def toValue(record: (LongWritable, MapWritable), name: String): Any = {
    record._2.get(new Text(name)) match {
      case d: DoubleWritable => d.get
      case f: FloatWritable => f.get
      case l: LongWritable => l.get
      case i: IntWritable => i.get
      case t: Text => t.toString
      case other => other // TODO throw exception or Option ?
    }
  }

  override def buildScan(requiredColumns: Array[String]) = baseRDD.map(record => {
    Row.fromSeq(requiredColumns.map { col => toValue(record, col) })
  })

}
