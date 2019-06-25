package com.dineshkb.threshold.flink.streams.sink

import com.dineshkb.threshold.domain.ThresholdControl
import org.apache.flink.streaming.api.functions.sink.SinkFunction

trait ThresholdControlSink extends SinkFunction[ThresholdControl]

object ThresholdControlSink {
  @throws(classOf[Exception])
  def apply(name: String): ThresholdControlSink = Class.forName(name).getConstructor().newInstance().asInstanceOf[ThresholdControlSink]
}


