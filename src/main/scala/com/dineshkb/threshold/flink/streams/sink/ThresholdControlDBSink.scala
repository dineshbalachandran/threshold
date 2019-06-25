package com.dineshkb.threshold.flink.streams.sink

import com.dineshkb.threshold.domain.ThresholdControl
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import scalikejdbc._

class ThresholdControlDBSink extends RichSinkFunction[ThresholdControl] with ThresholdControlSink {

  @transient
  val connectName = "controlsink" + Thread.currentThread.getId

  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    Class.forName(System.getProperty("sink.thresholdControl.db.class"))

    val settings = ConnectionPoolSettings(
      initialSize = System.getProperty("sink.thresholdControl.db.pool.initialSize").toInt,
      maxSize = System.getProperty("sink.thresholdControl.db.pool.maxSize").toInt,
      connectionTimeoutMillis = System.getProperty("sink.thresholdControl.db.pool.connectionTimeoutMillis").toLong,
      validationQuery = "select 1 from dual")

    ConnectionPool.add(connectName, System.getProperty("sink.thresholdControl.db.url"),
      System.getProperty("sink.thresholdControl.db.user"),
      System.getProperty("sink.thresholdControl.db.password"),
      settings)
  }

  @throws[Exception]
  override def invoke(r: ThresholdControl): Unit = {
    val status = "inprgrs"
    if (r.isFirst) {
      sql"insert into thresholdcontrol(id, breachStart, status, createdAt) values (${r.id}, ${r.breachStart}, ${status}, current_timestamp)"
        .update.apply()(NamedAutoSession(connectName))
    }

    sql"insert into thresholdcontrollevel(controlId, breachLevel, createdAt) select controlId, ${r.breachLevel}, current_timestamp from thresholdcontrol where id = ${r.id} and status = ${status}"
      .update.apply()(NamedAutoSession(connectName))
  }

  @throws[Exception]
  override def close(): Unit = {
    ConnectionPool.close(connectName)
  }
}
