package com.dineshkb.threshold.flink.job.test

import java.io.File

import com.dineshkb.threshold.domain.{OutEvent, ThresholdControl}
import com.dineshkb.threshold.flink.job.BreachIdentification
import net.liftweb.json.{DefaultFormats, parse}
import org.apache.flink.test.util.AbstractTestBase
import org.junit.Assert._
import org.junit.Test
import scalikejdbc._

class BreachIdentificationTest extends AbstractTestBase {

  //first level not satisfied, second and third levels satisfied - no breach
  @Test
  def testNoBreach(): Unit = {

    System.setProperty("source.inEvent.file.dataFilePath", """.\src\test\resources\breachidentification\noBreach\source_Nobreach.json""")
    System.setProperty("sink.outEvent.file.dataFilePath", """.\src\test\resources\breachidentification\noBreach\sink_Nobreach.json""")
    System.setProperty("sink.thresholdControl.file.dataFilePath", """.\src\test\resources\breachidentification\noBreach\controlsink_Nobreach.json""")

    cleanUp(List(System.getProperty("sink.thresholdControl.file.dataFilePath"),
      System.getProperty("sink.outEvent.file.dataFilePath")))

    BreachIdentification.main(Array("unit"))

    val breach = readOutEvent(System.getProperty("sink.outEvent.file.dataFilePath")).size
    val control = readThresholdControl(System.getProperty("sink.thresholdControl.file.dataFilePath")).size

    assertEquals(0, breach)
    assertEquals(0, control)
  }

  //first level satisfied, second and third levels satisfied as well - all three levels breached
  @Test
  def testAllLevelBreach(): Unit = {

    System.setProperty("source.inEvent.file.dataFilePath", """.\src\test\resources\breachidentification\allLevelBreach\source_AllLevelbreach.json""")
    System.setProperty("sink.outEvent.file.dataFilePath", """.\src\test\resources\breachidentification\allLevelBreach\sink_AllLevelbreach.json""")
    System.setProperty("sink.thresholdControl.file.dataFilePath", """.\src\test\resources\breachidentification\allLevelBreach\controlsink_AllLevelbreach.json""")

    cleanUp(List(System.getProperty("sink.thresholdControl.file.dataFilePath"),
      System.getProperty("sink.outEvent.file.dataFilePath")))

    BreachIdentification.main(Array("unit"))

    val breach = readOutEvent(System.getProperty("sink.outEvent.file.dataFilePath")).map(_.level).sorted
    val control = readThresholdControl(System.getProperty("sink.thresholdControl.file.dataFilePath")).map(_.breachLevel).sorted

    assertEquals(List(0, 1, 2), breach)
    assertEquals(List(0, 1, 2), control)
  }

  //first and third levels satisfied, second not satisfied - only 1st and 3rd levels should breach
  @Test
  def testMultiNotAllLevelBreach(): Unit = {

    System.setProperty("source.inEvent.file.dataFilePath", """.\src\test\resources\breachidentification\multiLevelBreach\source_MultiLevelbreach.json""")
    System.setProperty("sink.outEvent.file.dataFilePath", """.\src\test\resources\breachidentification\multiLevelBreach\sink_MultiLevelbreach.json""")
    System.setProperty("sink.thresholdControl.file.dataFilePath", """.\src\test\resources\breachidentification\multiLevelBreach\controlsink_MultiLevelbreach.json""")

    cleanUp(List(System.getProperty("sink.thresholdControl.file.dataFilePath"),
      System.getProperty("sink.outEvent.file.dataFilePath")))

    BreachIdentification.main(Array("unit"))

    val breach = readOutEvent(System.getProperty("sink.outEvent.file.dataFilePath")).map(_.level).sorted
    val control = readThresholdControl(System.getProperty("sink.thresholdControl.file.dataFilePath")).map(_.breachLevel).sorted

    assertEquals(List(0, 2), breach)
    assertEquals(List(0, 2), control)
  }

  //only first level satisfied for two different thresholds - only first level breached
  @Test
  def testFirstLevelBreach(): Unit = {

    System.setProperty("source.inEvent.file.dataFilePath", """.\src\test\resources\breachidentification\firstLevelBreach\source_FirstLevelbreach.json""")
    System.setProperty("sink.outEvent.file.dataFilePath", """.\src\test\resources\breachidentification\firstLevelBreach\sink_FirstLevelbreach.json""")
    System.setProperty("sink.thresholdControl.file.dataFilePath", """.\src\test\resources\breachidentification\firstLevelBreach\controlsink_FirstLevelbreach.json""")

    cleanUp(List(System.getProperty("sink.thresholdControl.file.dataFilePath"),
      System.getProperty("sink.outEvent.file.dataFilePath")))

    BreachIdentification.main(Array("unit"))

    val breach = readOutEvent(System.getProperty("sink.outEvent.file.dataFilePath")).map(x => (x.id, x.level)).sorted
    val control = readThresholdControl(System.getProperty("sink.thresholdControl.file.dataFilePath")).map(x => (x.id, x.breachLevel)).sorted

    assertEquals(List(("TH001", 0), ("TH002", 0)), breach)
    assertEquals(List(("TH001", 0), ("TH002", 0)), control)
  }

  //EoIs not mapped to a threshold should not trigger breach - no breach triggered
  @Test
  def testUnMappedEOI(): Unit = {

    System.setProperty("source.inEvent.file.dataFilePath", ".\\src\\test\\resources\\breachidentification\\unMappedEOI\\source_UnmappedEOI.json")
    System.setProperty("sink.outEvent.file.dataFilePath", ".\\src\\test\\resources\\breachidentification\\unMappedEOI\\sink_UnmappedEOI.json")
    System.setProperty("sink.thresholdControl.file.dataFilePath", ".\\src\\test\\resources\\breachidentification\\unMappedEOI\\controlsink_UnmappedEOI.json")

    cleanUp(List(System.getProperty("sink.thresholdControl.file.dataFilePath"),
      System.getProperty("sink.outEvent.file.dataFilePath")))

    BreachIdentification.main(Array("unit"))

    val breach = readOutEvent(System.getProperty("sink.outEvent.file.dataFilePath")).size
    val control = readThresholdControl(System.getProperty("sink.thresholdControl.file.dataFilePath")).size

    assertEquals(0, breach)
    assertEquals(0, control)
  }

  //tests threshold definition and control from a database instead of file, breaches all 3 levels
  @Test
  def testDBSink(): Unit = {
    ConnectionPool.add('controltest, "jdbc:h2:mem:threshold", "user", "pass")
    createTables()
    setUpData()

    System.setProperty("source.inEvent.file.dataFilePath", """.\src\test\resources\breachidentification\dbSink\source_AllLevelbreach_dbsink.json""")
    System.setProperty("sink.outEvent.file.dataFilePath", """.\src\test\resources\breachidentification\dbSink\sink_AllLevelbreach_dbsink.json""")
    cleanUp(List(System.getProperty("sink.outEvent.file.dataFilePath")))

    BreachIdentification.main(Array("unit.db"))

    val breach = readOutEvent(System.getProperty("sink.outEvent.file.dataFilePath")).map(_.level).sorted
    val control = getThresholdControl().map(_.breachLevel).sorted

    ConnectionPool.close('controltest)

    assertEquals(List(0, 1, 2), breach)
    assertEquals(List(0, 1, 2), control)
  }

  private def cleanUp(f: Seq[String]): Unit = {
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

  def getThresholdControl(): List[ThresholdControl] = {
    sql"""select c.id as id, c.breachStart as breachStart, l.breachLevel as breachLevel
        from (select controlId, breachLevel
               from thresholdcontrollevel) as l
        inner join thresholdcontrol as c on l.controlId = c.controlId where c.status = 'inprgrs'""".foldLeft(scala.collection.mutable.ListBuffer[ThresholdControl]())((l, rs) => {
      val id = rs.string("id")
      val control = ThresholdControl(id, rs.long("breachStart"), rs.int("breachLevel"))
      l += control
    })(NamedAutoSession('controltest)).toList
  }

  def createTables(): Unit = {
    sql"""
    create table threshold (
      id varchar(8) not null primary key,
      desc varchar(64),
      createdAt timestamp not null
    )""".execute().apply()(NamedAutoSession('controltest))

    sql"""
    create table thresholdmapping (
      id varchar(8) not null,
      eoiID varchar(8) not null,
      createdAt timestamp not null,
      foreign key (id) references threshold(id),
      primary key (id, eoiID)
    )""".execute().apply()(NamedAutoSession('controltest))

    sql"""
    create table thresholdlevel (
      id varchar(8) not null,
      level int not null,
      count smallint not null,
      duration int not null,
      createdAt timestamp not null,
      foreign key (id) references threshold(id),
      primary key (id, level)
    )""".execute().apply()(NamedAutoSession('controltest))

    sql"""
    create table thresholdcontrol (
      controlId identity not null primary key,
      id varchar(8) not null,
      breachStart bigint not null,
      status varchar(8) not null,
      createdAt timestamp not null,
      foreign key (id) references threshold(id),
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

  def setUpData(): Unit = {
    Seq(("TH001", "Desc001"), ("TH002", "Desc002")) foreach { th =>
      sql"insert into threshold (id, desc, createdAt) values (${th._1}, ${th._2}, current_timestamp)".update.apply()(NamedAutoSession('controltest))
    }

    Seq(("TH001", "EOI1"), ("TH001", "EOI2"), ("TH002", "EOI3"), ("TH002", "EOI4"), ("TH002", "EOI5")) foreach { th =>
      sql"insert into thresholdmapping (id, eoiId, createdAt) values (${th._1}, ${th._2}, current_timestamp)".update.apply()(NamedAutoSession('controltest))
    }

    Seq(("TH001", 0, 5, 5000), ("TH001", 1, 10, 10000), ("TH001", 2, 15, 20000), ("TH002", 0, 2, 5000), ("TH002", 1, 10, 10000), ("TH002", 2, 15, 20000)) foreach { th =>
      sql"insert into thresholdlevel (id, level, count, duration, createdAt) values (${th._1}, ${th._2}, ${th._3}, ${th._4}, current_timestamp)".update.apply()(NamedAutoSession('controltest))
    }

    Seq(("TH001", 1000, "closed"), ("TH002", 2000, "closed")) foreach { th =>
      sql"insert into thresholdcontrol (id, breachStart, status, createdAt) values (${th._1}, ${th._2}, ${th._3}, current_timestamp)".update.apply()(NamedAutoSession('controltest))
    }

    val l: List[Long] =
      sql"""select controlId from thresholdcontrol where id in ('TH001', 'TH002')""".foldLeft(List[Long]())((l, rs) => {
        rs.long("controlId") :: l
      })(NamedAutoSession('controltest))

    l.flatMap(x => List((x, 0), (x, 1))) foreach { th =>
      sql"insert into thresholdcontrollevel (controlId, breachLevel, createdAt) values (${th._1}, ${th._2}, current_timestamp)".update.apply()(NamedAutoSession('controltest))
    }
  }

  //TODO: add test for threshold control reload and closure
}
