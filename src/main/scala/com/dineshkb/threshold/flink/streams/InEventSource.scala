package com.dineshkb.threshold.flink.streams

import com.dineshkb.threshold.domain.InEvent
import org.apache.flink.streaming.api.functions.source.RichSourceFunction

trait InEventSource extends RichSourceFunction[InEvent]

object InEventSource {
  @throws(classOf[Exception])
  def apply(name: String): InEventSource = {
    name match {
      case "file" => new InEventFileSource
      case _ => throw new Exception(name + " related class not found")
    }
  }
}