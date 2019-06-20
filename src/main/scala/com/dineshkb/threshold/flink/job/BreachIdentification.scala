package com.dineshkb.threshold.flink.job

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.dineshkb.threshold.domain.{InEvent, OutEvent, ThresholdControl}
import com.dineshkb.threshold.flink.streams.{InEventSource, OutEventSink, ThresholdControlSink}
import com.dineshkb.threshold.flink.windowing.{AsyncThresholdEnricherFunction, BreachIdentificationFunction, ThresholdTrigger}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows

/**
  * You can also generate a .jar file that you can submit on your Flink
  * cluster. Just type
  * {{{
  *   sbt clean assembly
  * }}}
  * in the projects root directory. You will find the jar in
  * target/scala-2.11/Flink\ Project-assembly-0.1-SNAPSHOT.jar
  *
  */
object BreachIdentification {

  def main(args: Array[String]): Unit = {
    setProperties()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(System.getProperty("job.parallelism").toInt)
    env.getConfig.setAutoWatermarkInterval(System.getProperty("source.watermarkIntervalMillis").toLong)

    val src = InEventSource(System.getProperty("source.inEvent"))
    val eventSnk = OutEventSink(System.getProperty("sink.outEvent"))
    val cntrlSnk = ThresholdControlSink(System.getProperty("sink.thresholdControl"))

    //TODO: add custom timestamp and watermark assigner
    //TODO: add kinesis source and sink
    //TODO: add logging
    //TODO: Remove println statements

    val in: DataStream[InEvent] = env
      .addSource(src)
      .assignTimestampsAndWatermarks(
        new AssignerWithPeriodicWatermarks[InEvent] {
          val watermarkInterval: Long = System.getProperty("source.watermarkIntervalMillis").toLong
          var currentWaterMark: Long = -1L

          override def extractTimestamp(element: InEvent, previousElementTimestamp: Long): Long = {
            if (currentWaterMark == -1L)
              currentWaterMark = element.time - watermarkInterval
            element.time
          }

          override def getCurrentWatermark: Watermark = new Watermark(currentWaterMark + watermarkInterval)
        })

    val enriched = AsyncDataStream.unorderedWait(in, AsyncThresholdEnricherFunction(),
      System.getProperty("threshold.enricher.timeoutMillis").toLong, TimeUnit.MILLISECONDS,
      System.getProperty("threshold.enricher.capacity").toInt)

    val out: DataStream[OutEvent] = enriched
      .keyBy(_.thDef)
      .window(GlobalWindows.create())
      .trigger(ThresholdTrigger())
      .process(BreachIdentificationFunction())
      .filter(_.breached)

    val sidetag = OutputTag[ThresholdControl]("control")
    val side: DataStream[ThresholdControl] = out.getSideOutput(sidetag).filter(_.breached)

    out.addSink(eventSnk)
    side.addSink(cntrlSnk)

    env.execute("Threshold Breach Identification")
  }

  def setProperties(): Unit = {
    val in = getClass.getClassLoader.getResourceAsStream("application.properties")
    val prop = new Properties(System.getProperties)
    prop.load(in)
    System.setProperties(prop)
  }
}
