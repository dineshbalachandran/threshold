package com.dineshkb.threshold.flink.windowing

import com.dineshkb.threshold
import com.dineshkb.threshold.domain._
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

class BreachIdentificationFunction extends ProcessWindowFunction[EnrichedEvent, OutEvent, ThresholdDefinition, TimeWindow] {

  override def process(th: ThresholdDefinition, context: Context, elements: Iterable[EnrichedEvent], out: Collector[OutEvent]): Unit = {
    val thcState = context.globalState.getState(BreachIdentificationFunction.controlDesc)
    val eventState = context.globalState.getListState(BreachIdentificationFunction.elementsDesc)

    setInitialThresholdControlState(elements, thcState)

    if (!breachPreConditionsMet(th, thcState.value, elements))
      return

    val events = getEventsOrderedByTime(elements, eventState.get)

    getLevelsRemainingToBeBreached(th, thcState) foreach {
      thLevel => outputPotentialBreachEvent(thLevel, th, thcState, events, context, out)
    }

    updateEventsState(th, thcState, eventState, events, context)
  }

  private def getLevelsRemainingToBeBreached(th: ThresholdDefinition, thcState: ValueState[ThresholdControl]): List[ThresholdLevel] =
    th.levels.slice(thcState.value.breachLevel + 1, th.levels.size)


  private def updateEventsState(th: ThresholdDefinition, thcState: ValueState[ThresholdControl], eventState: ListState[InEvent], events: Vector[InEvent], context: Context): Unit = {
    val cutOff = if (thcState.value.breached) thcState.value.breachStart else context.window.getEnd - th.levels.head.duration
    eventState.update(events.filter(_.time >= cutOff).asJava)
  }

  private def updateThresholdControlState(thcState: ValueState[ThresholdControl], newThc: ThresholdControl): Unit = {
    thcState.update(newThc)
  }

  private def outputPotentialBreachEvent(thLevel: ThresholdLevel, th: ThresholdDefinition, thcState: ValueState[ThresholdControl], events: Vector[InEvent], context: Context, out: Collector[OutEvent]): Unit = {
    val outEvent = generateOutEvent(th, thLevel, events, thcState.value)
    if (outEvent.breached) {
      val newThc = generateNewThresholdControl(th, thcState.value, outEvent)

      out.collect(outEvent)
      context.output(OutputTag[ThresholdControl]("control"), newThc)

      updateThresholdControlState(thcState, newThc)
    }
  }

  /*This method considers the below 5 input conditions and how it updates the threshold control state
          Input						            Output
          Event		      State		      State
          Not Breached	Null		      Not Breached	(This is the usual start up condition or when an existing breach is closed)
          Not Breached	Not Breached	Not Breached	(no breach has occurred)
          Not Breached	Breached	    Breached	    (breach has occurred, however cache has not refreshed)
          Breached	    Breached	    Breached	    (breach has occurred, cache has refreshed)
          Breached      Null          Null          (this input condition indicates that an event came while a breach
                                                     is in progress and the state has expired.
                                                     In this case, retain the state as null as there is no need to
                                                     process (since no further breach levels are possible).
          */
  private def setInitialThresholdControlState(elements: Iterable[EnrichedEvent], thcState: ValueState[ThresholdControl]) : Unit = {
    if (thcState.value == null && elements.nonEmpty && !elements.head.thCtrl.breached)
      updateThresholdControlState(thcState, elements.head.thCtrl)
  }

  private def breachPreConditionsMet(th: ThresholdDefinition, thc: ThresholdControl, elements: Iterable[EnrichedEvent]): Boolean = {
    if (th == threshold.UNDEFINED ||
        elements.isEmpty          ||
        thc == null                  //a breach is in-progress and no further breaches are possible
       )
      false
    else
      true
  }

  private def getEventsOrderedByTime(elements: Iterable[EnrichedEvent], ls: java.lang.Iterable[InEvent]): Vector[InEvent] = {
    val l = new scala.collection.mutable.ListBuffer[InEvent]
    ls.forEach(l.append(_)) //ls is already sorted by time
    l ++= elements.map(_.inEvent).toSeq.sortBy(_.time)
    l.toVector
  }

  private def generateOutEvent(th: ThresholdDefinition, thLevel: ThresholdLevel, events: Vector[InEvent], thc: ThresholdControl): OutEvent = {
    if (thLevel.isFirst)
      firstBreach(0, thLevel.count - 1, th.id, events, thLevel)
    else {
      if (thc.breached)
        subsequentBreach(th.id, thLevel, events, thc)
      else
        threshold.NO_BREACH_EVENT
    }
  }

  private def generateNewThresholdControl(th: ThresholdDefinition, thc: ThresholdControl, o: OutEvent): ThresholdControl = {
    val breachStart = if (thc.breached) thc.breachStart else o.start
    ThresholdControl(th.id, breachStart, o.level)
  }

  /** a tail recursive sliding window implementation */
  @scala.annotation.tailrec
  private def firstBreach(begin: Int, end: Int, thId: String, events: Vector[InEvent], thLevel: ThresholdLevel): OutEvent = {
    if (end >= events.size)
      OutEvent(breached = false, thId, level = thLevel.level, events.size, events(begin).time, events.last.time, events.last.time - events(begin).time)
    else if (end - begin + 1 == thLevel.count && events(end).time <= events(begin).time + thLevel.duration)
      OutEvent(breached = true, thId, level = thLevel.level, end - begin + 1, events(begin).time, events(end).time, events(end).time - events(begin).time)
    else if (events(end).time > events(begin).time + thLevel.duration)
      firstBreach(begin + 1, end + 1, thId, events, thLevel)
    else
      firstBreach(begin, end + 1, thId, events, thLevel)
  }

  private def subsequentBreach(thId: String, thLevel: ThresholdLevel, events: Vector[InEvent], thc: ThresholdControl): OutEvent = {
    val eventsCount = events.size
    if (thLevel.count <= eventsCount && events(thLevel.count - 1).time <= events.head.time + thLevel.duration)
      OutEvent(breached = true, thId, thLevel.level, thLevel.count, events.head.time, events(thLevel.count - 1).time, events(thLevel.count - 1).time - events.head.time)
    else
      OutEvent(breached = false, thId, thLevel.level, eventsCount, events.head.time, events.last.time, events.last.time - events.head.time)
  }
}

object BreachIdentificationFunction {
  val elementsDesc = new ListStateDescriptor[InEvent]("elements", TypeInformation.of(new TypeHint[InEvent] {}))
  val controlDesc = new ValueStateDescriptor[ThresholdControl]("control", TypeInformation.of(new TypeHint[ThresholdControl] {}))

  val ttlConfig: StateTtlConfig = StateTtlConfig
    .newBuilder(Time.seconds(System.getProperty("state.timeToLiveSecs").toLong))
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .build

  //time to live configuration is used to reset the state and is required to recognise when an existing breach is closed
  elementsDesc.enableTimeToLive(ttlConfig)
  controlDesc.enableTimeToLive(ttlConfig)

  def apply(): BreachIdentificationFunction = new BreachIdentificationFunction
}