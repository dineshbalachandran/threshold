package com.dineshkb

import com.dineshkb.threshold.domain.{OutEvent, ThresholdControl, ThresholdDefinition}

package object threshold {
  val UNDEFINED = ThresholdDefinition("undefined", List(), List())
  val CNTRL_NOT_PRESENT = ThresholdControl("notpresent", -1, -1)
  val NO_BREACH_EVENT = OutEvent(breached = false, id="NA", level = -1, count = -1, start = -1, end = -1, duration = -1)
}
