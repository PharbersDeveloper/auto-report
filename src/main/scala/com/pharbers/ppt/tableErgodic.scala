package com.pharbers.ppt

import java.io.{FileInputStream, FileOutputStream, InputStream, OutputStream}

import org.apache.poi.openxml4j.opc.PackagePart
import org.apache.poi.xslf.usermodel.XMLSlideShow
import org.apache.poi.xssf.usermodel.{XSSFSheet, XSSFWorkbook}
import org.apache.spark.sql.DataFrame
import com.pharbers.common.executeCalculation

case class tableErgodic(path: String, sourceDF: DataFrame) {
    val fis: FileInputStream = new FileInputStream(path)
    val ppt: XMLSlideShow = new XMLSlideShow(fis)
    val excelEmbedding: PackagePart = ppt.getAllEmbedds.get(67)
    val is: InputStream = excelEmbedding.getInputStream
    val workbook: XSSFWorkbook = new XSSFWorkbook(is)
    val sheet: XSSFSheet = workbook.getSheetAt(0)

    val colcount: Int = sheet.getRow(2).getLastCellNum
    val rowcount: Int = sheet.getLastRowNum

    val infoMap: Map[String, String] = getInfo(sheet)

    Array.range(1, colcount).foreach { col =>
        Array.range(2, rowcount).foreach { row =>
            val ymstr = if (col < 4) infoMap("ymstr1")
            else infoMap("ymstr2")

            val argsMap: Map[String, String] = Map(
                "ymcount" -> infoMap("ymcount"),
                "ymstr" -> ymstr,
                "totalKey" -> infoMap("totalKey"),
                "col" -> sheet.getRow(1).getCell(col).toString,
                "displayName" -> sheet.getRow(row).getCell(0).toString
            )

            val value: String = executeCalculation(argsMap, sourceDF).getValue()
            sheet.getRow(row).getCell(col).setCellValue(value)
        }
    }

    //保存文件
    val os: OutputStream = excelEmbedding.getOutputStream
    workbook.write(os)
    os.close()
    workbook.close()

    val fileOutputStream: FileOutputStream = new FileOutputStream("test.pptx")
    ppt.write(fileOutputStream)
    fileOutputStream.close()

    //获取表信息
    def getInfo(sheet: XSSFSheet): Map[String, String] = {
        val mon1 = sheet.getRow(0).getCell(2).toString
        val mon2 = sheet.getRow(0).getCell(5).toString
        val totalKey = sheet.getRow(2).getCell(0).toString
        val tableKey: String = sheet.getRow(1).getCell(1).toString
        val ymcount = 12.toString
        val ymstr1 = mon1.takeRight(5)
        val ymstr2 = mon2.takeRight(5)
        Map("tableKey" -> tableKey, "ymcount" -> ymcount, "ymstr1" -> ymstr1, "ymstr2" -> ymstr2, "totalKey" -> totalKey)
    }
}
