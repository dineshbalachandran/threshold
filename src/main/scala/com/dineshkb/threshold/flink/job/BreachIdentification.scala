package com.dineshkb.threshold.flink.job

import java.util.concurrent.TimeUnit

import com.dineshkb.threshold.domain.{InEvent, OutEvent, ThresholdControl}
import com.dineshkb.threshold.flink.streams.{InEventSource, OutEventSink, ThresholdControlSink}
import com.dineshkb.threshold.flink.windowing.{AsyncThresholdEnricherFunction, BreachIdentificationFunction, ThresholdTrigger}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
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

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)


    //val propertiesFilePath = "/home/sam/flink/myjob.properties"
    //val parameter = ParameterTool.fromPropertiesFile(propertiesFilePath)

    testInit()

    val src = InEventSource(System.getProperty("source.inEvent"))
    val eventSnk = OutEventSink(System.getProperty("sink.outEvent"))
    val cntrlSnk = ThresholdControlSink(System.getProperty("sink.thresholdControl"))

    //TODO: update tests to check for control sink values
    //TODO: Use the ParameterTool to read in properties and remove the testInit() function
    //TODO: add custom timestamp and watermark assigner
    //TODO: add kinesis source and sink
    //TODO: add a DB persister class for threshold definition and control classes, possibly using slick
    //TODO: Move the test source and sink classes to the test folder
    //TODO: instantiate the loader, source and sink classes using class names

    val in: DataStream[InEvent] = env.addSource(src)
    val enriched = AsyncDataStream.unorderedWait(in, AsyncThresholdEnricherFunction(), 5000, TimeUnit.MILLISECONDS, 100)

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


  private def testInit(): Unit = {
    System.setProperty("source.file.dataFilePath", "c:\\flink\\input_nobreach.txt")
    System.setProperty("source.file.servingSpeed", "1.0")
    System.setProperty("source.file.maxDelaySecs", "0")

    System.setProperty("sink.file.dataFilePath", "c:\\flink\\out_nobreach.txt")

    System.setProperty("source", "file")
    System.setProperty("sink", "file")
    System.setProperty("controlsink", "file")
    System.setProperty("threshold.loader", "file")

    System.setProperty("threshold.loader.file.definition", "c:\\flink\\threshold.json")
    System.setProperty("threshold.loader.file.control.input", "c:\\flink\\controlin.json")
    System.setProperty("controlSink.file.dataFilePath", "c:\\flink\\controlout.json")
  }
}
