package com.pharbers

import com.pharbers.ppt.tableErgodic
import com.pharbers.report.lilly.format.sourceFileFormat
import com.pharbers.spark.phSparkDriver
import org.apache.spark.sql.DataFrame

object main extends App {
    lazy val sparkDriver: phSparkDriver = phSparkDriver("cui-test")
    import sparkDriver.ss.implicits._

    val path:String = "/data/lily/source_file_20181118/"
    val filelst = List("CECLOR TOTAL.CSV", "CIALIS.CSV", "EVISTA.CSV", "ONO.CSV", "PROZAC.CSV", "STRATTERA.CSV",
        "VANCOCIN.CSV", "ZYPREXA.CSV", "INSULIN.CSV", "GLP-1.CSV", "CIALIS BPH.CSV", "CYMBALTA CMP.CSV", "OLUMIANT RA.CSV")

    def loadFile(filePath: String): DataFrame = {
        val df: DataFrame = phSparkDriver(applicationName = "cui-test").ss.read.format("com.databricks.spark.csv")
            .option("header", "false")
            .option("delimiter", ",")
            .load(filePath)
        df
    }

    val sourceFileDF: DataFrame = filelst.map { f =>
        sourceFileFormat().formatDF(loadFile(path + f))
    }.reduce((totalResult, f) => totalResult union f)

    val bigTable: DataFrame = sourceFileFormat.apply().bigTableFormat(sourceFileDF)
    val dotTable:DataFrame = sparkDriver.readCsv("hdfs:///test/ADD_RATE_Test.csv")
    val allTable: DataFrame = sourceFileFormat().allTableFormat(bigTable, dotTable)

    val sourceDF: DataFrame = allTable.select("TC II SHORT DESC", "TC II DESC", "TC III SHORT DESC",
        "TC III DESC", "TC IV SHORT DESC", "ATC IV DESC", "COMPS_ABB", "COMPS DESC", "PRODUCT SHORT DESC", "PRODUCT DESC",
        "APP FORM 1 SHORT DESC", "APP FORM 1 DESC", "PACK SHORT DESC", "PACK DESC", "YM", "Sales", "ST-CNT-UNIT",
        "DT-DOS-UNIT", "UN-T-UNITS", "ADD RATE", "UNIT_TYPE", "DOT")

    val displayDF: DataFrame = sparkDriver.readCsv("/test/ANTIDEPRESSANTS_CHPA_ BRAND_1_MAPPING.csv")
        .withColumnRenamed("PRODUCT DESC", "PRODUCT_DESC_MARKET")
        .withColumnRenamed("PACK DESC", "PACK_DESC_MARKET")
        .withColumnRenamed("COMPS DESC", "COMPS_DESC_MARKET")
        .select("Display Name", "COMPS_DESC_MARKET", "PRODUCT_DESC_MARKET", "PACK_DESC_MARKET")
    val fullDF: DataFrame = sourceDF.join(displayDF, sourceDF("PRODUCT DESC") === displayDF("PRODUCT_DESC_MARKET") &&
        sourceDF("PACK DESC") === displayDF("PACK_DESC_MARKET"))

    val pptPath: String = "/Users/cui/worktemp/CHPA_Monthly.pptx"
    tableErgodic(pptPath, fullDF)
}
