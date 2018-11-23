package com.pharbers.calculation

import com.pharbers.calculation.utils.CalculationUtils
import com.pharbers.common.ym
import com.pharbers.pactions.actionbase._
import org.apache.spark.sql.DataFrame

object SomPercentage{
    def apply(args: MapArgs): SomPercentage = new SomPercentage(args)
}


//市场份额百分比
class SomPercentage(override val defaultArgs: pActionArgs) extends pActionTrait{
    override val name: String = "result"

    override def perform(pr: pActionArgs): pActionArgs = {
        val sourceDF: DataFrame = defaultArgs.asInstanceOf[MapArgs].get("sourceDF").asInstanceOf[DFArgs].get
        val displayName: String = defaultArgs.asInstanceOf[MapArgs].get("displayName").asInstanceOf[StringArgs].get
        val ymstr: String = defaultArgs.asInstanceOf[MapArgs].get("ymstr").asInstanceOf[StringArgs].get
        val ymcount: String = defaultArgs.asInstanceOf[MapArgs].get("ymcount").asInstanceOf[StringArgs].get
        val totalKey: String = defaultArgs.asInstanceOf[MapArgs].get("totalKey").asInstanceOf[StringArgs].get
        val ymDF: DataFrame = ym(ymstr, ymcount).getymDF()

        val standmkt: Double = CalculationUtils(sourceDF, ymDF, displayName).getSum()
        val total: Double = CalculationUtils(sourceDF, ymDF, totalKey).getSum()

        val result = ((standmkt/total)*100).toString
        StringArgs(result)
    }

}
