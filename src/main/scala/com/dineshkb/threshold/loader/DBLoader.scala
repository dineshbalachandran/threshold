package com.dineshkb.threshold.loader

import com.dineshkb.threshold.domain.{ThresholdControl, ThresholdDefinition, ThresholdLevel}
import scalikejdbc._

import scala.collection.mutable

class DBLoader extends Loader {

  @throws[Exception]
  override def open(): Unit = {
    Class.forName(System.getProperty("threshold.loader.db.class"))

    val settings = ConnectionPoolSettings(
      initialSize = System.getProperty("threshold.loader.db.pool.initialSize").toInt,
      maxSize = System.getProperty("threshold.loader.db.pool.maxSize").toInt,
      connectionTimeoutMillis = System.getProperty("threshold.loader.db.pool.connectionTimeoutMillis").toLong,
      validationQuery = "select 1 from dual")

    ConnectionPool.add('loader, System.getProperty("threshold.loader.db.url"),
      System.getProperty("threshold.loader.db.user"),
      System.getProperty("threshold.loader.db.password"),
      settings)
  }

  @throws[Exception]
  override def getDefinition(): Map[String, ThresholdDefinition] = {
    val mappings = sql"select * from thresholdmapping".foldLeft(scala.collection.mutable.Map[String, List[String]]())((m, rs) => {
      val id = rs.string("id")
      val maps = rs.string("eoiId") :: (if (m contains id) m(id) else List())
      m += (id -> maps)
    })(NamedAutoSession('loader))

    val levels = sql"select * from thresholdlevel order by id, level desc".foldLeft(scala.collection.mutable.Map[String, List[ThresholdLevel]]())((m, rs) => {
      val id = rs.string("id")
      val levels = ThresholdLevel(rs.int("count"), rs.int("duration")) :: (if (m contains id) m(id) else List())
      m += (id -> levels)
    })(NamedAutoSession('loader))

    mappings.foldLeft(scala.collection.mutable.Map[String, ThresholdDefinition]())((definitions, e) => {
      val (id, eoiIds) = (e._1, e._2)
      definitions += (id -> ThresholdDefinition(id, eoiIds, levels(id)))
    }).toMap
  }

  @throws[Exception]
  override def getControl(): mutable.Map[String, ThresholdControl] = {
    sql"""select c.id as id, c.breachStart as breachStart, c.status, l.breachLevel as breachLevel
        from (select controlId, max(breachLevel) as breachLevel
               from thresholdcontrollevel group by controlId) as l
        inner join thresholdcontrol as c on l.controlId = c.controlId where c.status = 'inprgrs'""".foldLeft(scala.collection.mutable.Map[String, ThresholdControl]())((m, rs) => {
      val id = rs.string("id")
      val control = ThresholdControl(id, rs.long("breachStart"), rs.int("breachLevel"))
      m += (id -> control)
    })(NamedAutoSession('loader))
  }

  @throws[Exception]
  override def close(): Unit = {
    ConnectionPool.close('loader)
  }
}
