package com.dineshkb.threshold.flink.streams

import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}

import com.dineshkb.threshold.domain.ThresholdControl
import net.liftweb.json.Serialization.write
import net.liftweb.json._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

class ThresholdControlFileSink extends RichSinkFunction[ThresholdControl] with ThresholdControlSink {

  private var dataFilePath: String = _


  @transient
  private var writer: BufferedWriter = _

  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    init()
  }

  private def init(): Unit = {
    dataFilePath = System.getProperty("sink.thresholdControl.file.dataFilePath")
    writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(dataFilePath, true)))
  }

  @throws[Exception]
  override def invoke(r: ThresholdControl): Unit = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    writer.write(write(r))
    writer.newLine()
  }

  override def close(): Unit = {
    writer.close()
  }
}
