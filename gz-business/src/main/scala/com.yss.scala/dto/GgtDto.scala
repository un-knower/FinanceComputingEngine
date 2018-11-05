package com.yss.scala.dto

/**
  *
  * @param ywlx
  * @param bdlx
  * @param qsrq
  * @param jsrq
  * @param xwh1
  * @param zqzh
  * @param zqdm1
  * @param qsbz
  * @param mmbz
  * @param wbhl
  * @param cjbh
  * @param sqbh
  * @param sl
  * @param cjsl
  * @param wbje
  * @param yhs
  * @param jyzf
  * @param jyf
  * @param syf
  * @param jsf
  * @param qtje
  * @param wbysf
  * @param ysfje
  * @param paraminfos
  */
case class HkJsmxModel(
                        ywlx:String,
                        bdlx:String,
                        qsrq:String,
                        jsrq:String,
                        xwh1 :String,
                        zqzh :String,
                        zqdm1:String,
                        qsbz :String,
                        mmbz:String,
                        wbhl:String,
                        cjbh:String,
                        sqbh:String,
                        sl:String,
                        cjsl:String,
                        wbje:String,
                        yhs:String,
                        jyzf:String,
                        jyf:String,
                        syf:String,
                        jsf:String,
                        qtje:String,
                        wbysf:String,
                        ysfje:String,
                        paraminfos:String
                      )

/**
  *
  * @param fsetId
  * @param Fdate
  * @param FinDate
  * @param FZqdm
  * @param FSzsh
  * @param Fjyxwh
  * @param Fbs
  * @param Fje
  * @param Fsl
  * @param Fyj
  * @param Fjsf
  * @param Fyhs
  * @param Fzgf
  * @param Fghf
  * @param Fgzlx
  * @param Fhggain
  * @param Ffxj
  * @param Fsssje
  * @param FZqbz
  * @param Fywbz
  * @param Fqsbz
  * @param Fqtf
  * @param Zqdm
  * @param Fjyfs
  * @param Fsh
  * @param Fzzr
  * @param Fchk
  * @param fzlh
  * @param ftzbz
  * @param Fqsghf
  * @param fgddm
  * @param Fjybz
  * @param Isrtgs
  * @param Fpartid
  * @param Fhtxh
  * @param Fcshtxh
  * @param Frzlv
  * @param Fcsghqx
  * @param Fsjly
  * @param Fbz
  */
case class JsmxResultModel(fsetId:String,
                           Fdate:String,
                           FinDate:String,
                           FZqdm:String,
                           FSzsh:String,
                           Fjyxwh:String,
                           Fbs:String,
                           Fje:String,
                           Fsl:String,
                           Fyj:String,
                           Fjsf:String,
                           Fyhs:String,
                           Fzgf:String,
                           Fghf:String,
                           Fgzlx:String,
                           Fhggain:String,
                           Ffxj:String,
                           Fsssje:String,
                           FZqbz:String,
                           Fywbz:String,
                           Fqsbz:String,
                           Fqtf:String,
                           Zqdm:String,
                           Fjyfs:String,
                           Fsh:String,
                           Fzzr:String,
                           Fchk:String,
                           fzlh:String,
                           ftzbz:String,
                           Fqsghf:String,
                           fgddm:String,
                           Fjybz:String,
                           Isrtgs:String,
                           Fpartid:String,
                           Fhtxh :String,
                           Fcshtxh:String,
                           Frzlv  :String,
                           Fcsghqx:String,
                           Fsjly :String,
                           Fbz:String)

/**
  *
  * @param fqsxw
  * @param fjflb
  * @param fzqlb
  * @param ffylb
  * @param ffyfs
  * @param fsh
  * @param fzzr
  * @param fchk
  * @param fstartDate
  */
case class CsxwfyModel(
                        fqsxw:String,
                        fjflb:String,
                        fzqlb:String,
                        ffylb:String,
                        ffyfs:String,
                        fsh:String,
                        fzzr:String,
                        fchk:String,
                        fstartDate:String
                      )

/**
  *
  * @param FQSDM
  * @param FQSMC
  * @param FSZSH
  * @param FQSXW
  * @param FXWLB
  * @param FSETCODE
  * @param FSH
  * @param FZZR
  * @param FCHK
  * @param FSTARTDATE
  */
case class CsqsxwModel(
                        FQSDM:String,
                        FQSMC:String,
                        FSZSH:String,
                        FQSXW:String,
                        FXWLB:String,
                        FSETCODE:String,
                        FSH:String,
                        FZZR:String,
                        FCHK:String,
                        FSTARTDATE:String
                      )

/**
  * 佣金Model
  * @param FDate
  * @param FInDate
  * @param FZqdm
  * @param FJyxwh
  * @param fzqbz
  * @param Fywbz
  * @param zqdm
  * @param fbs
  */
case class YjModel(
                    FDate: String,
                    FInDate: String,
                    FZqdm: String,
                    FJyxwh: String,
                    fzqbz: String,
                    Fywbz: String,
                    zqdm: String,
                    fbs: String,
                    fyj:String,
                    Fsssje:String
                  )

