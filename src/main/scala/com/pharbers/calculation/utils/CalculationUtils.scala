package com.pharbers.calculation.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

case class CalculationUtils(sourceDF: DataFrame, ymDF: DataFrame, displayName: String) {
    def getSum(): Double ={
        val result = sourceDF.filter(col("Display name") === displayName)
            .join(ymDF, sourceDF("YM") === ymDF("yms"))
            .select("DOT")
            .agg(Map("DOT" -> "sum"))
            .collectAsList().get(0).toString()
        result.substring(1, result.size-1).toDouble
    }
}
