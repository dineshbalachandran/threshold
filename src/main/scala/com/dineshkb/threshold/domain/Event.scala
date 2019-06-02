package com.dineshkb.threshold.domain

case class InEvent(eoiId: String, time: Long)

case class OutEvent(breached: Boolean, id: String, level: Int, count: Int, start: Long, end: Long, duration: Long)

case class EnrichedEvent(inEvent: InEvent, thDef: ThresholdDefinition, thCtrl: ThresholdControl)
