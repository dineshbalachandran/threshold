package com.dineshkb.threshold.flink.streams

import com.dineshkb.threshold.domain.OutEvent
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

trait OutEventSink extends RichSinkFunction[OutEvent]

object OutEventSink {
  @throws(classOf[Exception])
  def apply(name: String): OutEventSink = Class.forName(name).getConstructor().newInstance().asInstanceOf[com.dineshkb.threshold.flink.streams.OutEventSink]
}