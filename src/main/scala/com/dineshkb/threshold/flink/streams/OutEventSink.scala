package com.dineshkb.threshold.flink.streams

import com.dineshkb.threshold.domain.OutEvent
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

trait OutEventSink extends RichSinkFunction[OutEvent]

object OutEventSink {
  @throws(classOf[Exception])
  def apply(name: String): OutEventSink = {

    name match {
      case "file" => new OutEventFileSink
      case _ => throw new Exception(name + "related class not found")
    }
  }
}