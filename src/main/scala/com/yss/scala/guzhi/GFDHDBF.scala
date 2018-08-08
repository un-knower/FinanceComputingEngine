package com.yss.scala.guzhi

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.math.BigDecimal.RoundingMode

case class DGH00001(FDATE: String,
                    FINDATE: String,
                    FZQDM: String,
                    FSZSH: String,
                    FJYXWH: String,
                    FBJE: BigDecimal,
                    FSJE: BigDecimal,
                    FBSL: BigDecimal,
                    FSSL: BigDecimal,
                    FBYJ: BigDecimal,
                    FSYJ: BigDecimal,
                    FBJSF: BigDecimal,
                    FSJSF: BigDecimal,
                    FBYHS: BigDecimal,
                    FSYHS: BigDecimal,
                    FBZGF: BigDecimal,
                    FSZGF: BigDecimal,
                    FBGHF: BigDecimal,
                    FSGHF: BigDecimal,
                    FBGZLX: BigDecimal,
                    FSGZLX: BigDecimal,
                    FHGGAIN: BigDecimal,
                    FBFXJ: BigDecimal,
                    FSFXJ: BigDecimal,
                    FBSFJE: BigDecimal,
                    FSSSJE: BigDecimal,
                    FZQBZ: String,
                    FYWBZ: String,
                    FQSBZ: String,
                    FBQTF: BigDecimal,
                    FSQTF: BigDecimal,
                    ZQDM: String,
                    FJYFS: String,
                    FSH: String,
                    FZZR: String,
                    FCHK: String,
                    FZLH: String,
                    FTZBZ: String,
                    FBQSGHF: BigDecimal,
                    FSQSGHF: BigDecimal,
                    FGDDM: String)

object GFDHDBF {

  def main(args: Array[String]): Unit = {
    doIt()
  }

  private def doIt(): Unit = {
    import com.yss.scala.dbf._

    val spark = SparkSession.builder().appName("mytest").master("local[*]").getOrCreate()
    val df = spark.sqlContext.dbfFile("C:\\Users\\wuson\\Desktop\\new\\wenj\\dgh00001.dbf")
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
          val jsf = cjje.*(rateJS).*(zk)
          val yhs = cjje.*(rateYH)
          val zgf = cjje.*(rateZG)
          val ghf = cjje.*(rateGH)
          val fx = cjje.*(rateFXJ)
          val yj = cjje.*(rateYJ).-(zgf).-(ghf).-(yhs)
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
          FBje = sumCjje.setScale(2, RoundingMode.HALF_UP)
          FBsl = sumCjsl.setScale(2, RoundingMode.HALF_UP)
          FBjsf = sumJsf.setScale(2, RoundingMode.HALF_UP)
          FByhs = sumYhs.setScale(2, RoundingMode.HALF_UP)
          FBzgf = sumZgf.setScale(2, RoundingMode.HALF_UP)
          FBghf = sumGhf.setScale(2, RoundingMode.HALF_UP)
          FBFxj = sumFxj.setScale(2, RoundingMode.HALF_UP)
          FByj = sumYj.setScale(2, RoundingMode.HALF_UP)
        } else {
          FSje = sumCjje.setScale(2, RoundingMode.HALF_UP)
          FSsl = sumCjsl.setScale(2, RoundingMode.HALF_UP)
          FSjsf = sumJsf.setScale(2, RoundingMode.HALF_UP)
          FSyhs = sumYhs.setScale(2, RoundingMode.HALF_UP)
          FSzgf = sumZgf.setScale(2, RoundingMode.HALF_UP)
          FSghf = sumGhf.setScale(2, RoundingMode.HALF_UP)
          FSFxj = sumFxj.setScale(2, RoundingMode.HALF_UP)
          FSyj = sumYj.setScale(2, RoundingMode.HALF_UP)
        }
        val bcrq = fields(0)
        val FSzsh = "H"
        val Fjyxwh = fields(2)
        val FBsfje = FBje.+(FBjsf).+(FBzgf).+(FBghf).setScale(2, RoundingMode.HALF_UP)
        val FSssje = FSje.-(FSjsf).-(FSzgf).-(FSghf).-(FSyhs).setScale(2, RoundingMode.HALF_UP)
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

        DGH00001(bcrq, bcrq, ZqDm, FSzsh, Fjyxwh, FBje, FSje, FBsl, FSsl, FByj,
          FSyj, FBjsf, FSjsf, FByhs, FSyhs, FBzgf, FSzgf, FBghf, FSghf, FBgzlx,
          FSgzlx,FHGGAIN, FBFxj, FSFxj, FBsfje, FSssje, FZqbz, FYwbz, FQsbz, FBQtf, FSQtf,
          ZqDm, FJyFS, Fsh, Fzzr, Fchk, fzlh, ftzbz, FBQsghf, FSQsghf, FGddm)
    }

    value.toDF()
      .write.format("jdbc")
      .option("url", "jdbc:mysql://192.168.102.119:3306/JJCWGZ?useUnicode=true&characterEncoding=utf8")
      .option("dbtable", "HZJKQS")
      .option("user", "root")
      .option("password", "root1234")
      .mode(SaveMode.Append)
      .save()
  }

}
