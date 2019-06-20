package com.dineshkb.threshold.flink.streams

import com.dineshkb.threshold.domain.ThresholdControl
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

trait ThresholdControlSink extends RichSinkFunction[ThresholdControl]

object ThresholdControlSink {
  @throws(classOf[Exception])
  def apply(name: String): ThresholdControlSink = Class.forName(name).getConstructor().newInstance().asInstanceOf[com.dineshkb.threshold.flink.streams.ThresholdControlSink]
}


