package com.dineshkb.threshold.flink.streams

import com.dineshkb.threshold
import com.dineshkb.threshold.domain.{EnrichedEvent, InEvent, ThresholdControl, ThresholdDefinition}
import com.dineshkb.threshold.loader.Loader
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}

import scala.concurrent.{ExecutionContext, Future}

class AsyncThresholdEnricherFunction extends RichAsyncFunction[InEvent, EnrichedEvent] {

  @transient private var loader: Loader = _
  @transient private var controls: scala.collection.mutable.Map[String, ThresholdControl] = _
  @transient private var eoi2Th: Map[String, ThresholdDefinition] = _
  @transient private var lastRefreshed: Long = _
  @transient private var refreshInterval: Long = _

  @throws(classOf[Exception])
  override def open(parameters: Configuration): Unit = {
    refreshInterval = System.getProperty("threshold.cacheRefreshIntervalMillis").toLong
    loader = Loader(System.getProperty("threshold.loader"))
    loader.open()
    load()
  }

  private def load(): Unit = loader.synchronized {
    if (cacheStale()) {
      eoi2Th = mapEOI2ThresholdDefinition(loader.getDefinition())
      controls = loader.getControl()
      lastRefreshed = System.currentTimeMillis()
    }
  }

  private def mapEOI2ThresholdDefinition(definitions: Map[String, ThresholdDefinition]): Map[String, ThresholdDefinition] = {
    definitions.foldLeft(scala.collection.mutable.Map[String, ThresholdDefinition]())((eoiToTh, e) => {
      val (_, th) = e
      th.mapping.foreach(eoi => eoiToTh += (eoi -> th))
      eoiToTh
    }).toMap
  }

  private def cacheStale(): Boolean = System.currentTimeMillis() > lastRefreshed + refreshInterval

  override def asyncInvoke(input: InEvent, resultFuture: ResultFuture[EnrichedEvent]): Unit = {
    Future {
      if (cacheStale()) load()
      val thDef = if (eoi2Th contains input.eoiId) eoi2Th(input.eoiId) else threshold.UNDEFINED
      val thCtrl = if (controls contains thDef.id) controls(thDef.id) else threshold.CNTRL_NOT_PRESENT

      val enrichedEvent = EnrichedEvent(input, thDef, thCtrl)
      resultFuture.complete(Seq(enrichedEvent))
    }(ExecutionContext.global)
  }

  override def close(): Unit = loader.synchronized { loader.close() }
}

object AsyncThresholdEnricherFunction {
  def apply(): AsyncThresholdEnricherFunction = new AsyncThresholdEnricherFunction
}