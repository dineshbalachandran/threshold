package com.dineshkb

import com.dineshkb.threshold.domain.{ThresholdControl, ThresholdDefinition}

package object threshold {
  val UNDEFINED = ThresholdDefinition("undefined", List(), List())
  val CNTRL_NOT_PRESENT = ThresholdControl("notpresent", -1, -1, -1)
}
