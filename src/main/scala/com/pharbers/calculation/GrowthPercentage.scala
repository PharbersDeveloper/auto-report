package com.pharbers.calculation

import com.pharbers.calculation.utils.CalculationUtils
import com.pharbers.common.ym
import com.pharbers.pactions.actionbase._
import org.apache.spark.sql.DataFrame

object GrowthPercentage{
    def apply(args: MapArgs): GrowthPercentage = new GrowthPercentage(args)
}

class GrowthPercentage(override val defaultArgs: pActionArgs) extends pActionTrait{
    override val name: String = "result"

    override def perform(pr: pActionArgs): pActionArgs = {
        val sourceDF: DataFrame = defaultArgs.asInstanceOf[MapArgs].get("sourceDF").asInstanceOf[DFArgs].get
        val displayName: String = defaultArgs.asInstanceOf[MapArgs].get("displayName").asInstanceOf[StringArgs].get
        val ymstr: String = defaultArgs.asInstanceOf[MapArgs].get("ymstr").asInstanceOf[StringArgs].get
        val ymcount: String = defaultArgs.asInstanceOf[MapArgs].get("ymcount").asInstanceOf[StringArgs].get
        val ymDF: DataFrame = ym(ymstr, ymcount).getymDF()
        val month = ymstr.split(" ").head
        val lastYear: String = (ymstr.split(" ").last.toInt-1).toString
        val lastymstr: String = month + " " + lastYear
        val lastyearymDF: DataFrame = ym(lastymstr, ymcount).getymDF()

        val sum: Double = CalculationUtils(sourceDF, ymDF, displayName).getSum()
        val lastyearsum: Double = CalculationUtils(sourceDF, lastyearymDF, displayName).getSum()

        val reuslt = (((sum-lastyearsum)/lastyearsum)*100).toString
        StringArgs(reuslt)
    }

}
