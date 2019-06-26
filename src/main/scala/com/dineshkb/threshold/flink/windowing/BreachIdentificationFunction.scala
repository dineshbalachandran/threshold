package com.dineshkb.threshold.flink.windowing

import com.dineshkb.threshold
import com.dineshkb.threshold.domain._
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

class BreachIdentificationFunction extends ProcessWindowFunction[EnrichedEvent, OutEvent, ThresholdDefinition, TimeWindow] {

  val elementsDesc = new ListStateDescriptor[InEvent]("elements", TypeInformation.of(new TypeHint[InEvent] {}))
  val controlDesc = new ValueStateDescriptor[ThresholdControl]("control", TypeInformation.of(new TypeHint[ThresholdControl] {}))

  override def process(th: ThresholdDefinition, context: Context, elements: Iterable[EnrichedEvent], out: Collector[OutEvent]): Unit = {

    if (elements.isEmpty || th == threshold.UNDEFINED)
      return

    val thcState = getUpdatedThresholdControlState(elements.head.thCtrl, context)
    if (thcState.value == null)
      return

    val eventState = context.globalState.getListState(elementsDesc)

    val events = mergeElementsAndState(eventState, elements)
    th.levels.slice(thcState.value.breachLevel + 1, th.levels.size).foreach(x => {
      val outEvent = identifyBreach(th, events, thcState.value)
      val newThc = generateThresholdControl(th, thcState.value, outEvent)
      if (outEvent.breached) {
        out.collect(outEvent)
        context.output(OutputTag[ThresholdControl]("control"), newThc)
        thcState.update(newThc)
      }
    })

    val cutOff = if (thcState.value.breached) thcState.value.breachStart else context.currentWatermark - th.levels.head.duration
    eventState.update(events.filter(_.time >= cutOff).asJava)
  }

  /*This method considers the below 5 input conditions and updates the threshold control state
    Input						            Output
    Event		      State		      State
    Not Breached	Null		      Not Breached	(This is the usual condition on start up)
    Not Breached	Not Breached	Not Breached	(no breach has occurred)
    Not Breached	Breached	    Breached	    (breach has occurred, however cache has not synchronized)
    Breached	    Breached	    Breached	    (breach has occurred, cache has synchronized)
    Breached      Null          Null          (this input condition should ideally never occur, this means that
                                               the state expired and an event came while a breach is in progress
                                               in this case, retain the state as null as there is no need to
                                               process (no further triggers are possible)
    */
  private def getUpdatedThresholdControlState(thCtrlEvent: ThresholdControl, context: Context): ValueState[ThresholdControl] = {
    val thcState = context.globalState.getState(controlDesc)
    if (thcState.value == null && !thCtrlEvent.breached)
      thcState.update(thCtrlEvent)

    thcState
  }

  private def breachesPossible(th: ThresholdDefinition, thc: ThresholdControl): Boolean = thc.breachLevel < th.levels.size - 1

  //TODO: Optimize this code
  private def mergeElementsAndState(ls: ListState[InEvent], elements: Iterable[EnrichedEvent]): Vector[InEvent] = {
    val l = new scala.collection.mutable.ListBuffer[InEvent]
    l ++= elements.map(_.inEvent)
    ls.get.forEach(x => l += x)
    l.toVector.sortBy(_.time)
  }

  private def identifyBreach(th: ThresholdDefinition, events: Vector[InEvent], thc: ThresholdControl): OutEvent = {
    if (thc.breached) subsequentBreach(th, events, thc)
    else firstLevelBreach(th, events)
  }

  private def generateThresholdControl(th: ThresholdDefinition, thc: ThresholdControl, o: OutEvent): ThresholdControl = {
    if (o.breached) {
      val breachStart = if (thc.breached) thc.breachStart else o.start
      ThresholdControl(th.id, breachStart, o.level)
    } else
      thc
  }

  private def firstLevelBreach(th: ThresholdDefinition, events: Vector[InEvent]): OutEvent = {
    if (events.size < th.levels.head.count)
      firstlevel(0, events.size, th.id, events, th.levels.head)
    else
      firstlevel(0, th.levels.head.count - 1, th.id, events, th.levels.head)
  }

  /** a recursive sliding window implementation */
  private def firstlevel(start: Int, end: Int, id: String, events: Vector[InEvent], thLevel: ThresholdLevel): OutEvent = {
    if (end == events.size)
      OutEvent(breached = false, id, level = 0, events.size, events(start).time, events(end - 1).time, events(end - 1).time - events(start).time)
    else if (end - start + 1 == thLevel.count && events(end).time <= events(start).time + thLevel.duration)
      OutEvent(breached = true, id, level = 0, end - start + 1, events(start).time, events(end).time, events(end).time - events(start).time)
    else if (events(end).time > events(start).time + thLevel.duration)
      firstlevel(start + 1, end + 1, id, events, thLevel)
    else
      firstlevel(start, end + 1, id, events, thLevel)
  }

  private def subsequentBreach(th: ThresholdDefinition, events: Vector[InEvent], thc: ThresholdControl): OutEvent = {
    val eventsCount = events.size

    val level = th.levels.slice(thc.breachLevel + 1, th.levels.size).indexWhere(x => {
      if (x.count <= eventsCount && events(x.count - 1).time <= events.head.time + x.duration) true else false
    })

    level match {
      case -1 => OutEvent(breached = false, th.id, thc.breachLevel + 1, eventsCount, events.head.time, events.last.time, events.last.time - events.head.time)
      case _ =>
        val count = th.levels(thc.breachLevel + level + 1).count
        OutEvent(breached = true, th.id, thc.breachLevel + level + 1, count, events.head.time, events(count-1).time, events(count-1).time - events.head.time)
    }
  }
}


object BreachIdentificationFunction {
  def apply(): BreachIdentificationFunction = new BreachIdentificationFunction
}