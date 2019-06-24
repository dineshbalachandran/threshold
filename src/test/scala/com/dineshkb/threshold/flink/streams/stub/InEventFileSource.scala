package com.dineshkb.threshold.flink.streams.stub

import java.io.{BufferedReader, FileInputStream, IOException, InputStreamReader}
import java.util.Calendar

import com.dineshkb.threshold.domain.InEvent
import com.dineshkb.threshold.flink.streams.InEventSource
import net.liftweb.json.{DefaultFormats, parse}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.watermark.Watermark

class InEventFileSource extends InEventSource {

  private var dataFilePath: String = _
  private var maxDelayMsecs: Int = _
  private var servingSpeed: Float = _
  private var watermarkDelayMSecs: Int = _


  @transient
  private var reader: BufferedReader = _

  override def run(sourceContext: SourceContext[InEvent]): Unit = {
    generateStream(sourceContext)
  }

  override def open(parameters: Configuration): Unit = {
    dataFilePath = System.getProperty("source.inEvent.file.dataFilePath")
    maxDelayMsecs = 1000
    servingSpeed = 1.0f

    watermarkDelayMSecs = if (maxDelayMsecs < 10000) 10000 else maxDelayMsecs
    reader = new BufferedReader(new InputStreamReader(new FileInputStream(dataFilePath)))
  }

  @throws(classOf[IOException])
  private def generateOrderedStream(sourceContext: SourceContext[InEvent]): Unit = {
    val servingStartTime = Calendar.getInstance.getTimeInMillis
    var dataStartTime = 0L
    var nextWatermark = 0L
    var nextWatermarkServingTime = 0L

    implicit val formats: DefaultFormats.type = DefaultFormats

    if (reader.ready) {
      val line = reader.readLine
      if (line != null) {
        val json = parse(line)
        val event = json.extract[InEvent]

        dataStartTime = event.time
        nextWatermark = dataStartTime - 10000 + watermarkDelayMSecs
        nextWatermarkServingTime = toServingTime(servingStartTime, dataStartTime, nextWatermark)

        sourceContext.emitWatermark(new Watermark(dataStartTime - 10000))
        sourceContext.collectWithTimestamp(event, event.time)
      }
    } else {
      return
    }

    while (reader.ready) {
      val line = reader.readLine
      if (line != null) {
        val json = parse(line)
        val event = json.extract[InEvent]

        val eventTime = event.time
        val now = Calendar.getInstance.getTimeInMillis
        val eventServingTime = toServingTime(servingStartTime, dataStartTime, eventTime)

        val eventWait = eventServingTime - now
        val watermarkWait = nextWatermarkServingTime - now

        if (eventWait < watermarkWait) {
          Thread.sleep(if (eventWait > 0) eventWait else 0)
        } else if (eventWait > watermarkWait) {
          Thread.sleep(if (watermarkWait > 0) watermarkWait else 0)


          sourceContext.emitWatermark(new Watermark(nextWatermark))
          nextWatermark += watermarkDelayMSecs
          nextWatermarkServingTime = toServingTime(servingStartTime, dataStartTime, nextWatermark)

          val remainWait: Long = eventWait - watermarkWait
          Thread.sleep(if (remainWait > 0) remainWait else 0)
        } else if (eventWait == watermarkWait) {
          Thread.sleep(if (watermarkWait > 0) watermarkWait else 0)

          sourceContext.emitWatermark(new Watermark(nextWatermark))
          nextWatermark += watermarkDelayMSecs
          nextWatermarkServingTime = toServingTime(servingStartTime, dataStartTime, nextWatermark)
        }

        sourceContext.collectWithTimestamp(event, event.time)
      }
    }

    sourceContext.emitWatermark(new Watermark(nextWatermark))
  }

  def toServingTime(servingStartTime: Long, dataStartTime: Long, eventTime: Long): Long = {
    val dataDiff = eventTime - dataStartTime
    servingStartTime + (dataDiff / servingSpeed).toLong
  }

  @throws[Exception]
  override def close(): Unit = {
    try {
      if (reader != null) {
        reader.close()
      }
    } finally {
      reader = null
    }
  }

  @throws(classOf[IOException])
  override def cancel(): Unit = {
    close()
  }

  @throws(classOf[IOException])
  private def generateStream(sourceContext: SourceContext[InEvent]): Unit = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    while (reader.ready) {
      val line = reader.readLine
      if (line != null) {
        val json = parse(line)
        val event = json.extract[InEvent]
        sourceContext.collect(event)
      }
    }
  }

}
