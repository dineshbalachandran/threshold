package com.dineshkb.threshold.flink.job

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.dineshkb.threshold.domain.{InEvent, OutEvent, ThresholdControl}
import com.dineshkb.threshold.flink.streams.{InEventSource, OutEventSink, PunctuatedAssigner, ThresholdControlSink}
import com.dineshkb.threshold.flink.windowing.{AsyncThresholdEnricherFunction, BreachIdentificationFunction, ThresholdTrigger}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}

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
    setProperties(args(0))
    val env = getExecutionEnvironment()

    val src = InEventSource(System.getProperty("source.inEvent"))
    val eventSnk = OutEventSink(System.getProperty("sink.outEvent"))
    val cntrlSnk = ThresholdControlSink(System.getProperty("sink.thresholdControl"))

    //TODO: add kinesis source and sink
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
      .keyBy(_.thDef)
      .window(GlobalWindows.create())
      .trigger(ThresholdTrigger())
      .process(BreachIdentificationFunction())
      .uid("identifier-id")

    val sidetag = OutputTag[ThresholdControl]("control")
    val side: DataStream[ThresholdControl] = out.getSideOutput(sidetag)

    side.addSink(cntrlSnk).setParallelism(1)
    out.addSink(eventSnk).setParallelism(1)

    env.execute("Threshold Breach Identification")
  }

  private def getExecutionEnvironment(): StreamExecutionEnvironment = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(System.getProperty("job.parallelism").toInt)

    //checkpointing
    env.enableCheckpointing(System.getProperty("job.checkpointingMillis").toLong)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(System.getProperty("job.setMinPauseBetweenCheckpointsMillis").toLong)
    // checkpoints have to complete within one minute, or are discarded
    env.getCheckpointConfig.setCheckpointTimeout(System.getProperty("job.setCheckpointTimeoutMillis").toLong)
    // prevent the tasks from failing if an error happens in their checkpointing, the checkpoint will just be declined.
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
