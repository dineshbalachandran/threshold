package com.dineshkb.threshold.flink.streams

import java.io.{BufferedReader, FileInputStream, IOException, InputStreamReader}
import java.util.Calendar

import com.dineshkb.threshold.domain.InEvent
import net.liftweb.json.{DefaultFormats, parse}
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.watermark.Watermark

class InEventFileSource extends RichSourceFunction[InEvent] with InEventSource {

  private var dataFilePath: String = _
  private var maxDelayMsecs: Int = _
  private var servingSpeed: Float = _
  private var watermarkDelayMSecs: Int = _

  @transient
  private var reader: BufferedReader = _

  override def run(sourceContext: SourceContext[InEvent]): Unit = {
    init()

    generateOrderedStream(sourceContext)

    reader.close()
    reader = null
  }

  private def init(): Unit = {
    dataFilePath = System.getProperty("source.inEvent.file.dataFilePath")
    maxDelayMsecs = System.getProperty("source.inEvent.file.maxDelaySecs", "2").toInt * 1000
    servingSpeed = System.getProperty("source.inEvent.file.servingSpeed", "1.0").toFloat

    //val t = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]

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
        nextWatermark = dataStartTime - 25000 + watermarkDelayMSecs
        nextWatermarkServingTime = toServingTime(servingStartTime, dataStartTime, nextWatermark)
        println(dataStartTime - 25000 + ":" + Thread.currentThread().getId())
        sourceContext.emitWatermark(new Watermark(dataStartTime - 25000))
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

          println(nextWatermark + ":" + Thread.currentThread().getId())
          sourceContext.emitWatermark(new Watermark(nextWatermark))
          nextWatermark += watermarkDelayMSecs
          nextWatermarkServingTime = toServingTime(servingStartTime, dataStartTime, nextWatermark)

          val remainWait: Long = eventWait - watermarkWait
          Thread.sleep(if (remainWait > 0) remainWait else 0)
        } else if (eventWait == watermarkWait) {
          Thread.sleep(if (watermarkWait > 0) watermarkWait else 0)
          println(nextWatermark)
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

  @throws(classOf[IOException])
  override def cancel(): Unit = {
    try {
      if (reader != null) {
        reader.close()
      }
    } finally {
      reader = null
    }
  }

  @throws(classOf[IOException])
  private def generateStream(sourceContext: SourceContext[InEvent]): Unit = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    while (reader.ready) {
      val line = reader.readLine
      if (line != null) {
        val json = parse(line)
        val event = json.extract[InEvent]
        sourceContext.collectWithTimestamp(event, event.time)
      }
    }
  }

}
