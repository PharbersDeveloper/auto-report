package com.pharbers.report.lilly.action

import com.pharbers.pactions.generalactions.readCsvNoTitleAction
import org.apache.spark.sql.DataFrame

class readFileAction(path: String) {
    val sourceDF = readCsvNoTitleAction(path)
}
