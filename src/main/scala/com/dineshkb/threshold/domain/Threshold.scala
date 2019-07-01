package com.dineshkb.threshold.domain

case class ThresholdLevel(level: Int, count: Int, duration: Int) {
  def isFirst : Boolean = level == 0
}

case class ThresholdDefinition(id: String, mapping: List[String], levels: List[ThresholdLevel]) {
  override def equals(that: Any): Boolean =
    that match {
      case that: ThresholdDefinition => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }

  def canEqual(a: Any): Boolean = a.isInstanceOf[ThresholdDefinition]

  override def hashCode(): Int = id.hashCode
}

case class ThresholdControl(id: String, breachStart: Long, breachLevel: Int) {
  def breached: Boolean = breachLevel > -1

  def isFirst: Boolean = breachLevel == 0
}