package com.dineshkb.threshold.flink.streams

import com.dineshkb.threshold.domain.ThresholdControl
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

trait ThresholdControlSink extends RichSinkFunction[ThresholdControl]

object ThresholdControlSink {
  @throws(classOf[Exception])
  def apply(name: String): ThresholdControlSink = {

    name match {
      case "file" => new ThresholdControlFileSink
      case _ => throw new Exception(name + "related class not found")
    }
  }
}


