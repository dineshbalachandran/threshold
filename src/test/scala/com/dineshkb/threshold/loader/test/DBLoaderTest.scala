package com.dineshkb.threshold.loader.test

import com.dineshkb.threshold.loader.Loader
import org.scalatest.{FlatSpec, Matchers, Outcome}
import scalikejdbc._

class DBLoaderTest extends FlatSpec with Matchers {

  val loader = Loader("com.dineshkb.threshold.loader.DBLoader")

  override def withFixture(test: NoArgTest): Outcome = {
    setProperties()
    ConnectionPool.add('loadertest, "jdbc:h2:mem:loader", "user", "pass")
    createTables()
    setUpData()

    loader.open()
    try super.withFixture(test)
    finally {
      loader.close()
      ConnectionPool.close('loadertest)
    }
  }

  behavior of "DB loader class"

  it should "output the two thresholds" in {
    val m = loader.getDefinition()
    println(m)

    m.size shouldEqual 2
  }

  it should "output the one threshold control" in {
    val m = loader.getControl()
    println(m)

    m.size shouldEqual 1
  }

  def setProperties(): Unit = {
    System.setProperty("threshold.loader.db.class", "org.h2.Driver")
    System.setProperty("threshold.loader.db.url", "jdbc:h2:mem:threshold")
    System.setProperty("threshold.loader.db.user", "user")
    System.setProperty("threshold.loader.db.password", "pass")

    System.setProperty("threshold.loader.db.pool.initialSize", "1")
    System.setProperty("threshold.loader.db.pool.maxSize", "1")
    System.setProperty("threshold.loader.db.pool.connectionTimeoutMillis", "3000")
  }

  def createTables(): Unit = {
    sql"""
    create table threshold (
      id varchar(8) not null primary key,
      desc varchar(64),
      createdAt timestamp not null
    )""".execute().apply()(NamedAutoSession('loadertest))

    sql"""
    create table thresholdmapping (
      id varchar(8) not null,
      eoiID varchar(8) not null,
      createdAt timestamp not null,
      foreign key (id) references threshold(id),
      primary key (id, eoiID)
    )""".execute().apply()(NamedAutoSession('loadertest))

    sql"""
    create table thresholdlevel (
      id varchar(8) not null,
      level int not null,
      count smallint not null,
      duration int not null,
      createdAt timestamp not null,
      foreign key (id) references threshold(id),
      primary key (id, level)
    )""".execute().apply()(NamedAutoSession('loadertest))

    sql"""
    create table thresholdcontrol (
      controlId identity not null primary key,
      id varchar(8) not null,
      breachStart bigint not null,
      status varchar(8) not null,
      createdAt timestamp not null,
      foreign key (id) references threshold(id),
      unique (controlId, id)
    )""".execute().apply()(NamedAutoSession('loadertest))

    sql"""
    create table thresholdcontrollevel (
      controlId bigint not null,
      breachLevel int not null,
      createdAt timestamp not null,
      foreign key (controlId) references thresholdcontrol(controlId),
      primary key (controlId, breachlevel)
    )""".execute().apply()(NamedAutoSession('loadertest))
  }

  def setUpData(): Unit = {
    Seq(("TH001", "Desc001"), ("TH002", "Desc002")) foreach { th =>
      sql"insert into threshold (id, desc, createdAt) values (${th._1}, ${th._2}, current_timestamp)".update.apply()(NamedAutoSession('loadertest))
    }

    Seq(("TH001", "EOI1"), ("TH001", "EOI2"), ("TH001", "EOI3"), ("TH002", "EOI4")) foreach { th =>
      sql"insert into thresholdmapping (id, eoiId, createdAt) values (${th._1}, ${th._2}, current_timestamp)".update.apply()(NamedAutoSession('loadertest))
    }

    Seq(("TH001", 1, 5, 5), ("TH001", 2, 10, 10), ("TH001", 3, 15, 20), ("TH002", 1, 1, 1), ("TH002", 2, 5, 5)) foreach { th =>
      sql"insert into thresholdlevel (id, level, count, duration, createdAt) values (${th._1}, ${th._2}, ${th._3}, ${th._4}, current_timestamp)".update.apply()(NamedAutoSession('loadertest))
    }

    Seq(("TH001", 1000, "closed"), ("TH002", 2000, "inprgrs")) foreach { th =>
      sql"insert into thresholdcontrol (id, breachStart, status, createdAt) values (${th._1}, ${th._2}, ${th._3}, current_timestamp)".update.apply()(NamedAutoSession('loadertest))
    }

    val l: List[Long] =
      sql"""select controlId from thresholdcontrol where id in ('TH001', 'TH002')""".foldLeft(List[Long]())((l, rs) => {
        rs.long("controlId") :: l
      })(NamedAutoSession('loadertest))

    l.flatMap(x => List((x, 1), (x, 2))) foreach { th =>
      sql"insert into thresholdcontrollevel (controlId, breachLevel, createdAt) values (${th._1}, ${th._2}, current_timestamp)".update.apply()(NamedAutoSession('loadertest))
    }
  }
}
