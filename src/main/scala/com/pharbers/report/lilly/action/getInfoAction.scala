package com.pharbers.report.lilly.action

import org.apache.poi.xssf.usermodel.XSSFSheet
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class getInfoAction(sheet: XSSFSheet, sourceDF: DataFrame, mappingDF: DataFrame) {
    val re = " ".r
    val yms: List[String] = List(sheet.getRow(2).toString, sheet.getRow(5).toString)
        .map(i => re.replaceAllIn(i.substring(5), "/"))
    val colnum: Int = sheet.getRow(2).getLastCellNum

    def getCell(key1: String, key2: String, ym: String, sourceDF: DataFrame, mappingDF: DataFrame): Unit = {
        val filteredMappingDF = mappingDF.filter(col("Display Name") === key2)
            .withColumnRenamed("PRODUCT DESC", "PRODUCT_DESC_MAP")
            .withColumnRenamed("PACK DESC", "PACK_DESC_MAP")
            .withColumnRenamed("MOLECULE DESC", "MOLECULE_DESC_MAP")
        val filteredSourceDF = sourceDF.filter(col("ym") === ym)
        val tableDF = filteredSourceDF.join(filteredMappingDF, filteredMappingDF("PRODUCT DESC") === filteredMappingDF("PRODUCT_DESC_MAP") &&
            filteredMappingDF("PACK DESC") === filteredMappingDF("PACK_DESC_MAP"))

        val typelst: List[String] = List("DOT", "SOM", "Growth")
    }

    Array.range(2, sheet.getLastRowNum + 1).foreach { i =>
        val key = sheet.getRow(i).getCell(0).toString
        Array.range(1, colnum).foreach { j =>
            val ym = if (j <= 3) yms.head
            else yms.last
            getCell(key, sheet.getRow(1).getCell(j).toString, ym, sourceDF, mappingDF)
        }
    }
    sheet
}
