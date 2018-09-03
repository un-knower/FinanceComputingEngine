package com.yss.scala.guzhi

import com.yss.scala.dto.ShghDto
import com.yss.scala.util.Util
import org.apache.spark.sql.SparkSession

import scala.math.BigDecimal.RoundingMode


/**
  * @author ws
  * @version 2018-08-08
  *          描述：上海大宗过户
  *          源文件：gdh.dbf
  *          结果表：HZJKQS
  */
object Shdzgh {

  def main(args: Array[String]): Unit = {
    doIt()
  }

  private def doIt(): Unit = {
    import com.yss.scala.dbf._

    val spark = SparkSession.builder().appName("SHDZGH").master("local[*]").getOrCreate()
//    val df = spark.sqlContext.dbfFile(Util.getInputFilePath("dgh00001.dbf"))
    val df = spark.sqlContext.dbfFile("C:\\Users\\wuson\\Desktop\\new\\data\\dgh2250120180418.dbf")
    import spark.implicits._

    val value = df.rdd.map(row => {
      val bcrq = row.getAs[String]("BCRQ")
      val zqdm = row.getAs[String]("ZQDM")
      val gsdm = row.getAs[String]("GSDM")
      val fgddm = row.getAs[String]("GDDM")
      val bs = row.getAs[String]("BS")
      val key = bcrq + "-" + zqdm + "-" + gsdm + "-" + fgddm + "-" + bs
      (key, row)
    }).groupByKey().map {
      case (key, values) =>
        val fields = key.split("-")
        var FBje = BigDecimal(0)
        var FSje = BigDecimal(0)
        var FBsl = BigDecimal(0)
        var FSsl = BigDecimal(0)
        var FByj = BigDecimal(0)
        var FSyj = BigDecimal(0)
        var FBjsf = BigDecimal(0)
        var FSjsf = BigDecimal(0)
        var FByhs = BigDecimal(0)
        var FSyhs = BigDecimal(0)
        var FBzgf = BigDecimal(0)
        var FSzgf = BigDecimal(0)
        var FBghf = BigDecimal(0)
        var FSghf = BigDecimal(0)
        val FBgzlx = BigDecimal(0)
        val FSgzlx = BigDecimal(0)
        var FBFxj = BigDecimal(0)
        var FSFxj = BigDecimal(0)
        val bs = fields(4)

        //默认费率
        val rateYJ = BigDecimal(0.0026)
        val rateJS = BigDecimal(0.00012)
        val zk = BigDecimal(0.7)
        val rateYH = BigDecimal(0.0011)
        val rateZG = BigDecimal(0.000041)
        val rateGH = BigDecimal(0.00051)
        val rateFXJ = BigDecimal(0.000036)

        var sumCjje = BigDecimal(0)
        var sumCjsl = BigDecimal(0)
        var sumYj = BigDecimal(0)
        var sumJsf = BigDecimal(0)
        var sumYhs = BigDecimal(0)
        var sumZgf = BigDecimal(0)
        var sumGhf = BigDecimal(0)
        var sumFxj = BigDecimal(0)
        for (row <- values) {
          val cjje = BigDecimal(row.getAs[String]("CJJE"))
          val cjsl = BigDecimal(row.getAs[String]("CJSL"))

          val jsf = cjje.*(rateJS).*(zk).setScale(2,RoundingMode.HALF_UP)
          var yhs = BigDecimal(0)
          if("S".equals(bs)){
            yhs = cjje.*(rateYH).setScale(2,RoundingMode.HALF_UP)
          }

          val zgf = cjje.*(rateZG).setScale(2,RoundingMode.HALF_UP)
          val ghf = cjsl.*(rateGH).setScale(2,RoundingMode.HALF_UP)
          val fx = cjje.*(rateFXJ).setScale(2,RoundingMode.HALF_UP)
          val yj = cjje.*(rateYJ).setScale(2,RoundingMode.HALF_UP).-(zgf).-(jsf)
          sumCjje = sumCjje.+(cjje)
          sumCjsl = sumCjsl.+(cjsl)
          sumYj = sumYj.+(yj)
          sumJsf = sumJsf.+(jsf)
          sumYhs = sumYhs.+(yhs)
          sumZgf = sumZgf.+(zgf)
          sumGhf = sumGhf.+(ghf)
          sumFxj = sumFxj.+(fx)
        }

        if ("B".equals(bs)) {
          FBje = sumCjje
          FBsl = sumCjsl
          FBjsf = sumJsf
//          FByhs = sumYhs.setScale(2, RoundingMode.HALF_UP)
          FBzgf = sumZgf
          FBghf = sumGhf
          FBFxj = sumFxj
          FByj = sumYj
        } else {
          FSje = sumCjje
          FSsl = sumCjsl
          FSjsf = sumJsf
          FSyhs = sumYhs
          FSzgf = sumZgf
          FSghf = sumGhf
          FSFxj = sumFxj
          FSyj = sumYj
        }
        val bcrq = fields(0)
        val FSzsh = "H"
        val Fjyxwh = fields(2)
        val FBsfje = -FBje.+(FBjsf).+(FBzgf).+(FBghf)
        val FSssje = FSje.-(FSjsf).-(FSzgf).-(FSghf).-(FSyhs)
        val FZqbz = "GP"
        val FYwbz = "DZ"
        val FQsbz = "N"
        val FBQtf = BigDecimal(0)
        val FSQtf = BigDecimal(0)
        val ZqDm = fields(1)
        val FJyFS = "PT"
        val Fsh = "1"
        val Fzzr = " "
        val Fchk = " "
        val fzlh = "0"
        val ftzbz = ""
        val FBQsghf = BigDecimal(0)
        val FSQsghf = BigDecimal(0)
        val FGddm = fields(3)
        val FHGGAIN = BigDecimal(0)

        ShghDto(bcrq, bcrq, ZqDm, FSzsh, Fjyxwh, FBje.formatted("%.2f"), FSje.formatted("%.2f"), FBsl.formatted("%.2f"), FSsl.formatted("%.2f"), FByj.formatted("%.2f"),
          FSyj.formatted("%.2f"), FBjsf.formatted("%.2f"), FSjsf.formatted("%.2f"), FByhs.formatted("%.2f"), FSyhs.formatted("%.2f"), FBzgf.formatted("%.2f"), FSzgf.formatted("%.2f"), FBghf.formatted("%.2f"), FSghf.formatted("%.2f"), FBgzlx.formatted("%.2f"),
          FSgzlx.formatted("%.2f"),  FBFxj.formatted("%.2f"), FSFxj.formatted("%.2f"), FBsfje.formatted("%.2f"), FSssje.formatted("%.2f"), FZqbz, FYwbz, FQsbz, FBQtf.formatted("%.2f"), FSQtf.formatted("%.2f"),
          ZqDm, FJyFS, Fsh, Fzzr, Fchk, fzlh, ftzbz, FBQsghf.formatted("%.2f"), FSQsghf.formatted("%.2f"), FGddm,FHGGAIN.formatted("%.2f"))
    }
    Util.outputMySql(value.toDF(), "SHDZGH")
    spark.stop()

  }

}
