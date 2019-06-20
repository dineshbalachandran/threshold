package com.dineshkb.threshold.flink.job.test

import java.io.File
import java.util.concurrent.TimeUnit

import com.dineshkb.threshold.domain.{InEvent, OutEvent, ThresholdControl}
import com.dineshkb.threshold.flink.streams.{InEventSource, OutEventSink, ThresholdControlSink}
import com.dineshkb.threshold.flink.windowing.{AsyncThresholdEnricherFunction, BreachIdentificationFunction, ThresholdTrigger}
import net.liftweb.json.{DefaultFormats, parse}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.test.util.AbstractTestBase
import org.junit.Assert._
import org.junit.Test
import scalikejdbc._

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

  @Test
  def testDBSink(): Unit = {
    commonDBInit()
    ConnectionPool.add('controltest, "jdbc:h2:mem:controltest", "user", "pass")
    createTables()

    cleanUp(List(System.getProperty("sink.outEvent.file.dataFilePath")))

    execute()

    val breach = readOutEvent(System.getProperty("sink.outEvent.file.dataFilePath"))
      .filter(x => {
        x.breached
      }).map(_.level).sorted
    val control = getThresholdControl()
      .filter(x => {
        x.breached
      }).map(_.breachLevel).sorted

    ConnectionPool.close('controltest)

    assertEquals(List(0, 1, 2), breach)
    assertEquals(List(0, 1, 2), control)
  }

  private def execute(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val src = InEventSource(System.getProperty("source.inEvent"))
    val eventSnk = OutEventSink(System.getProperty("sink.outEvent"))
    val cntrlSnk = ThresholdControlSink(System.getProperty("sink.thresholdControl"))

    //val in: DataStream[InEvent] = env.addSource(src)

    //TODO: remove dependency on system time
    val in: DataStream[InEvent] = env
      .addSource(src)
      .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[InEvent] {
        val watermarkDelay = 10000L
        var nextWaterMarkTime: Long = -1L
        var maxTime: Long = -1L

        override def extractTimestamp(element: InEvent, previousElementTimestamp: Long): Long = {
          maxTime = if (element.time > maxTime) element.time else maxTime
          element.time
        }

        override def checkAndGetNextWatermark(lastElement: InEvent, extractedTimestamp: Long): Watermark = {
          nextWaterMarkTime = if (nextWaterMarkTime < 0) extractedTimestamp - watermarkDelay else nextWaterMarkTime
          if (maxTime >= nextWaterMarkTime + watermarkDelay) {
            val w = new Watermark(nextWaterMarkTime)
            nextWaterMarkTime += watermarkDelay
            w
          } else {
            null
          }
        }
      })

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

    System.setProperty("source.inEvent", "com.dineshkb.threshold.flink.streams.stub.InEventFileSource")
    System.setProperty("sink.outEvent", "com.dineshkb.threshold.flink.streams.stub.OutEventFileSink")
    System.setProperty("sink.thresholdControl", "com.dineshkb.threshold.flink.streams.stub.ThresholdControlFileSink")
    System.setProperty("threshold.loader", "com.dineshkb.threshold.loader.stub.FileLoader")
    System.setProperty("threshold.loader.file.definition", """.\src\test\resources\breachidentification\definition.json""")
    System.setProperty("threshold.loader.file.control", """.\src\test\resources\breachidentification\control.json""")

    System.setProperty("threshold.cacheRefreshIntervalMillis", "5000")
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

  private def readThresholdControl(dataFilePath: String): List[ThresholdControl] = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    val reader = scala.io.Source.fromFile(dataFilePath)
    val i = reader.getLines().map[ThresholdControl](x => parse(x).extract[ThresholdControl]).toList
    reader.close()
    i
  }

  private def commonDBInit(): Unit = {
    System.setProperty("source.inEvent", "com.dineshkb.threshold.flink.streams.stub.InEventFileSource")
    System.setProperty("sink.outEvent", "com.dineshkb.threshold.flink.streams.stub.OutEventFileSink")
    System.setProperty("sink.thresholdControl", "com.dineshkb.threshold.flink.streams.ThresholdControlDBSink")
    System.setProperty("source.inEvent.file.servingSpeed", "1.0")
    System.setProperty("source.inEvent.file.maxDelaySecs", "0")

    System.setProperty("source.inEvent.file.dataFilePath", """.\src\test\resources\breachidentification\source_AllLevelbreach.json""")
    System.setProperty("sink.outEvent.file.dataFilePath", """.\src\test\resources\breachidentification\sink_AllLevelbreach.json""")

    System.setProperty("threshold.loader", "com.dineshkb.threshold.loader.stub.FileLoader")
    System.setProperty("threshold.loader.file.definition", """.\src\test\resources\breachidentification\definition.json""")
    System.setProperty("threshold.loader.file.control", """.\src\test\resources\breachidentification\control.json""")

    System.setProperty("sink.thresholdControl.db.class", "org.h2.Driver")
    System.setProperty("sink.thresholdControl.db.url", "jdbc:h2:mem:controltest")
    System.setProperty("sink.thresholdControl.db.user", "user")
    System.setProperty("sink.thresholdControl.db.password", "pass")
    System.setProperty("sink.thresholdControl.db.pool.initialSize", "1")
    System.setProperty("sink.thresholdControl.db.pool.maxSize", "1")
    System.setProperty("sink.thresholdControl.db.pool.connectionTimeoutMillis", "3000")

    System.setProperty("threshold.cacheRefreshIntervalMillis", "5000")
  }

  def createTables(): Unit = {
    sql"""
    create table thresholdcontrol (
      controlId identity not null primary key,
      id varchar(8) not null,
      breachStart bigint not null,
      status varchar(8) not null,
      createdAt timestamp not null,
      unique (controlId, id)
    )""".execute().apply()(NamedAutoSession('controltest))

    sql"""
    create table thresholdcontrollevel (
      controlId bigint not null,
      breachLevel int not null,
      createdAt timestamp not null,
      foreign key (controlId) references thresholdcontrol(controlId),
      primary key (controlId, breachlevel)
    )""".execute().apply()(NamedAutoSession('controltest))
  }

  def getThresholdControl(): List[ThresholdControl] = {
    sql"""select c.id as id, c.breachStart as breachStart, l.breachLevel as breachLevel
        from (select controlId, breachLevel
               from thresholdcontrollevel) as l
        inner join thresholdcontrol as c on l.controlId = c.controlId""".foldLeft(scala.collection.mutable.ListBuffer[ThresholdControl]())((l, rs) => {
      val id = rs.string("id")
      val control = ThresholdControl(id, rs.long("breachStart"), rs.int("breachLevel"))
      l += control
    })(NamedAutoSession('controltest)).toList
  }

  //TODO: add test for threshold control reload and closure
}
