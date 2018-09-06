package com.yss.scala.dbf


import org.apache.spark.sql.SQLContext


package object dbf {

  /**
    * Adds a method, `dbfFile`, to SQLContext that allows reading data stored in DBF.
    */
  implicit class DBFSQLContext(sqlContext: SQLContext) {
    def dbfFile(filePath: String) =
      sqlContext.baseRelationToDataFrame(DBFRelation(filePath)(sqlContext))
  }

}
