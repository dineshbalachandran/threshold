package com.dineshkb.threshold.flink.job

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.dineshkb.threshold.domain.{InEvent, OutEvent, ThresholdControl}
import com.dineshkb.threshold.flink.streams.sink.{OutEventSink, ThresholdControlSink}
import com.dineshkb.threshold.flink.streams.source.InEventSource
import com.dineshkb.threshold.flink.streams.{AsyncThresholdEnricherFunction, PunctuatedAssigner}
import com.dineshkb.threshold.flink.windowing.BreachIdentificationFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}

object BreachIdentification {

  def main(args: Array[String]): Unit = {
    setProperties(args(0))
    val env = getExecutionEnvironment

    val src = InEventSource(System.getProperty("source.inEvent"))
    val eventSnk = OutEventSink(System.getProperty("sink.outEvent"))
    val cntrlSnk = ThresholdControlSink(System.getProperty("sink.thresholdControl"))

    val watermarkDelay = System.getProperty("source.watermarkDelayMillis").toLong

    //TODO: add logging

    val in: DataStream[InEvent] = env
      .addSource(src)
      .uid("source-id")
      .assignTimestampsAndWatermarks(new PunctuatedAssigner)
      .uid("watermark-id")

    val enriched = AsyncDataStream.unorderedWait(in, AsyncThresholdEnricherFunction(),
                    System.getProperty("threshold.enricher.timeoutMillis").toLong, TimeUnit.MILLISECONDS,
                    System.getProperty("threshold.enricher.capacity").toInt)

    val out: DataStream[OutEvent] = enriched
      .uid("enriched-source-id")
      .keyBy(_.thDef)
      .window(TumblingEventTimeWindows.of(Time.milliseconds(watermarkDelay)))
      .process(BreachIdentificationFunction())
      .uid("identifier-id")

    val sidetag = OutputTag[ThresholdControl]("control")
    val side: DataStream[ThresholdControl] = out.getSideOutput(sidetag)

    side.addSink(cntrlSnk).setParallelism(1)
    out.addSink(eventSnk).setParallelism(1)

    env.execute("Threshold Breach Identification")
  }

  private def getExecutionEnvironment: StreamExecutionEnvironment = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(System.getProperty("job.parallelism").toInt)

    env.enableCheckpointing(System.getProperty("job.checkpointingMillis").toLong)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(System.getProperty("job.setMinPauseBetweenCheckpointsMillis").toLong)
    env.getCheckpointConfig.setCheckpointTimeout(System.getProperty("job.setCheckpointTimeoutMillis").toLong)
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    env
  }

  @throws[Exception]
  def setProperties(env: String): Unit = {
    val in = getClass.getClassLoader.getResourceAsStream("application." + env + ".properties")
    val prop = new Properties(System.getProperties)
    prop.load(in)
    System.setProperties(prop)
  }
}
