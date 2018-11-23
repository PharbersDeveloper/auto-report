package com.pharbers.common

import com.pharbers.spark.phSparkDriver
import org.apache.spark.sql.DataFrame

case class ym(ymstr: String, ymcount: String) {
    lazy val sparkDriver: phSparkDriver = phSparkDriver()
    import sparkDriver.ss.implicits._

    def getymDF(): DataFrame = {
            val ym = ymstr.split(" ")
            val month = ym(0).toInt
            val year = 2000 + ym(1).toInt
            getymlst(List(month.toString + "/" + year.toString), month, year, ymcount.toInt).map { str =>
                if (str.length == 7) str
                else "0" + str
            }.toDF("yms")
    }

    def getymlst(ymlst: List[String], month: Int, year: Int, ymcount: Int): List[String] = {
        if (ymlst.size == ymcount) {
            ymlst
        } else {
            if (month == 1) getymlst(ymlst ::: List(month + "/" + year), 12, year - 1, ymcount)
            else getymlst(ymlst ::: List(month + "/" + year), month - 1, year, ymcount)
        }
    }
}
