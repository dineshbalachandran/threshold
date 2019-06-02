package com.dineshkb.threshold.flink.job.test

import java.io.File
import java.util.concurrent.TimeUnit

import com.dineshkb.threshold.domain.{InEvent, OutEvent, ThresholdControl}
import com.dineshkb.threshold.flink.streams.{InEventSource, OutEventSink, ThresholdControlSink}
import com.dineshkb.threshold.flink.windowing.{AsyncThresholdEnricherFunction, BreachIdentificationFunction, ThresholdTrigger}
import net.liftweb.json.{DefaultFormats, parse}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.test.util.AbstractTestBase
import org.junit.Assert._
import org.junit.Test

class BreachIdentificationTest extends AbstractTestBase {

  //first level not satisfied, second and third levels satisfied - no breach
  @Test
  def testNoBreach(): Unit = {
    commonInit()
    System.setProperty("source.inEvent.file.dataFilePath", """.\src\test\resources\breachidentification\source_Nobreach.json""")
    System.setProperty("sink.outEvent.file.dataFilePath", """.\src\test\resources\breachidentification\sink_Nobreach.json""")
    System.setProperty("sink.thresholdControl.file.dataFilePath", """.\src\test\resources\breachidentification\controlsink_Nobreach.json""")


    cleanUp(List(System.getProperty("sink.thresholdControl.file.dataFilePath"),
      System.getProperty("sink.outEvent.file.dataFilePath")))
    execute()

    val breach = readOutEvent(System.getProperty("sink.outEvent.file.dataFilePath")).count(x => {
      x.breached
    })
    val control = readThresholdControl(System.getProperty("sink.thresholdControl.file.dataFilePath")).count(x => {
      x.breached
    })
    assertEquals(0, breach)
    assertEquals(0, control)
  }

  private def execute(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val src = InEventSource(System.getProperty("source.inEvent"))
    val eventSnk = OutEventSink(System.getProperty("sink.outEvent"))
    val cntrlSnk = ThresholdControlSink(System.getProperty("sink.thresholdControl"))

    val in: DataStream[InEvent] = env.addSource(src)
    val enriched = AsyncDataStream.unorderedWait(in, new AsyncThresholdEnricherFunction, 1000, TimeUnit.MILLISECONDS, 100)

    val out: DataStream[OutEvent] = enriched
      .keyBy(_.thDef)
      .window(GlobalWindows.create())
      .trigger(ThresholdTrigger())
      .process(BreachIdentificationFunction())

    val sidetag = OutputTag[ThresholdControl]("control")
    val side: DataStream[ThresholdControl] = out.getSideOutput(sidetag)

    side.addSink(cntrlSnk)
    out.addSink(eventSnk)

    env.execute()
  }

  private def commonInit(): Unit = {

    System.setProperty("source.inEvent.file.servingSpeed", "1.0")
    System.setProperty("source.inEvent.file.maxDelaySecs", "0")

    System.setProperty("source.inEvent", "file")
    System.setProperty("sink.outEvent", "file")
    System.setProperty("sink.thresholdControl", "file")
    System.setProperty("threshold.loader", "file")

    System.setProperty("threshold.loader.file.definition", """.\src\test\resources\breachidentification\definition.json""")
    System.setProperty("threshold.loader.file.control", """.\src\test\resources\breachidentification\control.json""")
  }

  private def cleanUp(f: List[String]): Unit = {
    f.foreach(new File(_).delete())
  }

  private def readOutEvent(dataFilePath: String): List[OutEvent] = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    val reader = scala.io.Source.fromFile(dataFilePath)
    val i = reader.getLines().map[OutEvent](x => parse(x).extract[OutEvent]).toList
    reader.close()
    i
  }

  //TODO: add test for threshold control reload and closure

  private def readThresholdControl(dataFilePath: String): List[ThresholdControl] = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    val reader = scala.io.Source.fromFile(dataFilePath)
    val i = reader.getLines().map[ThresholdControl](x => parse(x).extract[ThresholdControl]).toList
    reader.close()
    i
  }

  //first level satisfied, second and third levels satisfied as well - all three levels breached
  @Test
  def testAllLevelBreach(): Unit = {
    commonInit()
    System.setProperty("source.inEvent.file.dataFilePath", """.\src\test\resources\breachidentification\source_AllLevelbreach.json""")
    System.setProperty("sink.outEvent.file.dataFilePath", """.\src\test\resources\breachidentification\sink_AllLevelbreach.json""")
    System.setProperty("sink.thresholdControl.file.dataFilePath", """.\src\test\resources\breachidentification\controlsink_AllLevelbreach.json""")

    cleanUp(List(System.getProperty("sink.thresholdControl.file.dataFilePath"),
      System.getProperty("sink.outEvent.file.dataFilePath")))

    execute()

    val breach = readOutEvent(System.getProperty("sink.outEvent.file.dataFilePath"))
      .filter(x => {
        x.breached
      }).map(_.level).sorted
    val control = readThresholdControl(System.getProperty("sink.thresholdControl.file.dataFilePath"))
      .filter(x => {
        x.breached
      }).map(_.breachLevel).sorted

    assertEquals(List(0, 1, 2), breach)
    assertEquals(List(0, 1, 2), control)
  }

  //first and third levels satisfied, second not satisfied - only 1st and 3rd levels should breach
  @Test
  def testMultiNotAllLevelBreach(): Unit = {
    commonInit()
    System.setProperty("source.inEvent.file.dataFilePath", """.\src\test\resources\breachidentification\source_MultiLevelbreach.json""")
    System.setProperty("sink.outEvent.file.dataFilePath", """.\src\test\resources\breachidentification\sink_MultiLevelbreach.json""")
    System.setProperty("sink.thresholdControl.file.dataFilePath", """.\src\test\resources\breachidentification\controlsink_MultiLevelbreach.json""")

    cleanUp(List(System.getProperty("sink.thresholdControl.file.dataFilePath"),
      System.getProperty("sink.outEvent.file.dataFilePath")))

    execute()

    val breach = readOutEvent(System.getProperty("sink.outEvent.file.dataFilePath"))
      .filter(x => {
        x.breached
      }).map(_.level).sorted
    val control = readThresholdControl(System.getProperty("sink.thresholdControl.file.dataFilePath"))
      .filter(x => {
        x.breached
      }).map(_.breachLevel).sorted

    assertEquals(List(0, 2), breach)
    assertEquals(List(0, 2), control)
  }

  //only first level satisfied for two different thresholds - only first level breached
  @Test
  def testFirstLevelBreach(): Unit = {
    commonInit()
    System.setProperty("source.inEvent.file.dataFilePath", """.\src\test\resources\breachidentification\source_FirstLevelbreach.json""")
    System.setProperty("sink.outEvent.file.dataFilePath", """.\src\test\resources\breachidentification\sink_FirstLevelbreach.json""")
    System.setProperty("sink.thresholdControl.file.dataFilePath", """.\src\test\resources\breachidentification\controlsink_FirstLevelbreach.json""")

    cleanUp(List(System.getProperty("sink.thresholdControl.file.dataFilePath"),
      System.getProperty("sink.outEvent.file.dataFilePath")))

    execute()

    val breach = readOutEvent(System.getProperty("sink.outEvent.file.dataFilePath"))
      .filter(x => {
        x.breached
      }).map(x => (x.id, x.level)).sorted
    val control = readThresholdControl(System.getProperty("sink.thresholdControl.file.dataFilePath"))
      .filter(x => {
        x.breached
      }).map(x => (x.id, x.breachLevel)).sorted

    assertEquals(List(("TH001", 0), ("TH002", 0)), breach)
    assertEquals(List(("TH001", 0), ("TH002", 0)), control)
  }

  //EoIs not mapped to a threshold should not trigger breach - no breach triggered
  @Test
  def testUnMappedEOI(): Unit = {
    commonInit()
    System.setProperty("source.inEvent.file.dataFilePath", """.\src\test\resources\breachidentification\source_UnmappedEOI.json""")
    System.setProperty("sink.outEvent.file.dataFilePath", """.\src\test\resources\breachidentification\sink_UnmappedEOI.json""")
    System.setProperty("sink.thresholdControl.file.dataFilePath", """.\src\test\resources\breachidentification\controlsink_UnmappedEOI.json""")

    cleanUp(List(System.getProperty("sink.thresholdControl.file.dataFilePath"),
      System.getProperty("sink.outEvent.file.dataFilePath")))

    execute()

    val breach = readOutEvent(System.getProperty("sink.outEvent.file.dataFilePath")).count(x => {
      x.breached
    })
    val control = readThresholdControl(System.getProperty("sink.thresholdControl.file.dataFilePath")).count(x => {
      x.breached
    })

    assertEquals(0, breach)
    assertEquals(0, control)
  }
}
