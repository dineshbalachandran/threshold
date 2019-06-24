package com.dineshkb.threshold.flink.streams

import com.dineshkb.threshold.domain.InEvent
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

class PunctuatedAssigner extends AssignerWithPunctuatedWatermarks[InEvent]{
  val watermarkDelay = System.getProperty("source.watermarkDelayMillis").toLong
  var nextWaterMarkTime: Long = -1L
  var maxTime: Long = -1L

  override def checkAndGetNextWatermark(lastElement: InEvent, extractedTimestamp: Long): Watermark = {
    nextWaterMarkTime = if (nextWaterMarkTime < 0) extractedTimestamp - watermarkDelay else nextWaterMarkTime
    if (maxTime >= nextWaterMarkTime + watermarkDelay) {
      println("a:" + nextWaterMarkTime + " m:" + maxTime + " t:" + Thread.currentThread.getId)
      val w = new Watermark(nextWaterMarkTime)
      nextWaterMarkTime += watermarkDelay
      w
    } else {
      null
    }
  }

  override def extractTimestamp(element: InEvent, previousElementTimestamp: Long): Long = {
    maxTime = if (element.time > maxTime) element.time else maxTime
    element.time
  }
}
