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

    if (elements.isEmpty || th == threshold.UNDEFINED)
      return

    val thcState = getUpdatedThresholdControlState(elements.head.thCtrl, context)
    if (thcState.value == null)
      return

    val eventState = context.globalState.getListState(BreachIdentificationFunction.elementsDesc)

    val events = mergeElementsAndState(eventState, elements)
    th.levels.slice(thcState.value.breachLevel + 1, th.levels.size).foreach(thLevel => {
      val outEvent = generateBreachEvent(th, thLevel, events, thcState.value)
      if (outEvent.breached) {
        val newThc = generateNewThresholdControl(th, thcState.value, outEvent)
        out.collect(outEvent)
        context.output(OutputTag[ThresholdControl]("control"), newThc)
        thcState.update(newThc)
      }
    })

    val cutOff = if (thcState.value.breached) thcState.value.breachStart else context.window.getEnd - th.levels.head.duration
    eventState.update(events.filter(_.time >= cutOff).asJava)
  }

  /*This method considers the below 5 input conditions and how it updates the threshold control state
    Input						            Output
    Event		      State		      State
    Not Breached	Null		      Not Breached	(This is the usual start up condition or when an existing breach is closed)
    Not Breached	Not Breached	Not Breached	(no breach has occurred)
    Not Breached	Breached	    Breached	    (breach has occurred, however cache has not synchronized)
    Breached	    Breached	    Breached	    (breach has occurred, cache has synchronized)
    Breached      Null          Null          (this input condition indicates that an event came while a breach
                                               is in progress and the state has expired.
                                               In this case, retain the state as null as there is no need to
                                               process (since no further breaches are possible).
    */
  private def getUpdatedThresholdControlState(thCtrlEvent: ThresholdControl, context: Context): ValueState[ThresholdControl] = {
    val thcState = context.globalState.getState(BreachIdentificationFunction.controlDesc)
    if (thcState.value == null && !thCtrlEvent.breached)
      thcState.update(thCtrlEvent)

    thcState
  }

  private def mergeElementsAndState(ls: ListState[InEvent], elements: Iterable[EnrichedEvent]): Vector[InEvent] = {
    val l = new scala.collection.mutable.ListBuffer[InEvent]
    ls.get.forEach(l.append(_)) //ls is sorted
    l ++= elements.map(_.inEvent).toSeq.sortBy(_.time)
    l.toVector
  }

  private def generateBreachEvent(th: ThresholdDefinition, thLevel: ThresholdLevel, events: Vector[InEvent], thc: ThresholdControl): OutEvent = {
    if (thLevel.isFirst)
      firstBreach(th.id, thLevel, events)
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

  private def firstBreach(thId: String, thLevel: ThresholdLevel, events: Vector[InEvent]): OutEvent = {
    if (events.size < thLevel.count)
      findFirstBreach(0, events.size, thId, events, thLevel)
    else
      findFirstBreach(0, thLevel.count - 1, thId, events, thLevel)
  }

  /** a recursive sliding window implementation */
  private def findFirstBreach(start: Int, end: Int, thId: String, events: Vector[InEvent], thLevel: ThresholdLevel): OutEvent = {
    if (end == events.size)
      OutEvent(breached = false, thId, level = thLevel.level, events.size, events(start).time, events(end - 1).time, events(end - 1).time - events(start).time)
    else if (end - start + 1 == thLevel.count && events(end).time <= events(start).time + thLevel.duration)
      OutEvent(breached = true, thId, level = thLevel.level, end - start + 1, events(start).time, events(end).time, events(end).time - events(start).time)
    else if (events(end).time > events(start).time + thLevel.duration)
      findFirstBreach(start + 1, end + 1, thId, events, thLevel)
    else
      findFirstBreach(start, end + 1, thId, events, thLevel)
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