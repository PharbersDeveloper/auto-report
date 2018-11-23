package com.pharbers.common

import org.apache.spark.sql.DataFrame
import com.pharbers.common.implCalculation
import com.pharbers.pactions.actionbase._

case class executeCalculation(args: Map[String, String], sourceDF: DataFrame) {
    //Growth(%)的空格不能少
    val classMap: Map[String, String] = Map(
        "DOT(Mn)" -> "com.pharbers.calculation.DotMn",
        "SOM(%)" -> "com.pharbers.calculation.SomPercentage",
        "  Growth(%)" -> "com.pharbers.calculation.GrowthPercentage"
    )
    val methordKey: String = args("col")
    def getValue(): String ={
        val initArgs: Map[String, pActionArgs] = args.map{i =>
            Map(i._1 -> StringArgs(i._2))
        }.reduce((totalMap, oneMap) => totalMap++oneMap) ++ Map("sourceDF" -> DFArgs(sourceDF))
        val constructor = Class.forName(classMap(methordKey)).getConstructors()(0)
        val result: String = constructor.newInstance(MapArgs(initArgs)).asInstanceOf[pActionTrait].perform(MapArgs(Map().empty))
                .asInstanceOf[StringArgs].get
        result
    }
}
