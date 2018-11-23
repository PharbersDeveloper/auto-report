package com.pharbers.common

import com.pharbers.pactions.actionbase.{pActionArgs, pActionTrait}

case class implCalculation() {
    def impl(clazz: String, initArgs: Map[String, pActionArgs]): pActionTrait = {
        val constructor = Class.forName(clazz).getConstructors()(0)
        constructor.newInstance(initArgs).asInstanceOf[pActionTrait]
    }
}
