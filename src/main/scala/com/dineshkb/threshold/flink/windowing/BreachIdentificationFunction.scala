package com.dineshkb.threshold.flink.windowing

import com.dineshkb.threshold.domain._
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

class BreachIdentificationFunction extends ProcessWindowFunction[EnrichedEvent, OutEvent, ThresholdDefinition, GlobalWindow] {

  val elementsDesc = new ListStateDescriptor[InEvent]("elements", TypeInformation.of(new TypeHint[InEvent] {}))
  val controlDesc = new ValueStateDescriptor[ThresholdControl]("control", TypeInformation.of(new TypeHint[ThresholdControl] {}))

  override def process(th: ThresholdDefinition, context: Context, elements: Iterable[EnrichedEvent], out: Collector[OutEvent]): Unit = {

    val ls = context.windowState.getListState(elementsDesc)
    val cs = context.windowState.getState(controlDesc)
    val thc = cs.value

    println("f" + context.currentWatermark)

    val events = mergeElementsAndState(ls, elements, th, thc, context)
    val outEvent = identifyBreach(th, events, thc)
    val newThc = generateThresholdControl(th, thc, outEvent, context)

    out.collect(outEvent)
    if (outEvent.breached)
      context.output(OutputTag[ThresholdControl]("control"), newThc)

    ls.update(events.asJava)
    cs.update(newThc)
  }

  private def mergeElementsAndState(ls: ListState[InEvent], elements: Iterable[EnrichedEvent], th: ThresholdDefinition, thc: ThresholdControl, context: Context) = {
    val cutOff = if (thc.breached) thc.breachStart else context.currentWatermark - th.levels.head.duration
    val l = new scala.collection.mutable.ListBuffer[InEvent]

    l ++= elements.filter(x => x.inEvent.time >= cutOff).map(_.inEvent)
    ls.get.forEach(x => if (x.time >= cutOff) l += x)

    l.toList.sortBy(_.time)
  }

  private def identifyBreach(th: ThresholdDefinition, events: List[InEvent], thc: ThresholdControl): OutEvent = {
    if (events.isEmpty) breachNA(th)
    else if (thc.breached) subsequentBreach(th, events, thc)
    else firstLevelBreach(th, events)
  }

  private def breachNA(th: ThresholdDefinition): OutEvent = OutEvent(breached = false, th.id, level = 0, -1, -1, -1, -1)

  //TODO: instead of end 0 pass end th.levels.head.count after the recent changes are tested
  private def firstLevelBreach(th: ThresholdDefinition, events: List[InEvent]): OutEvent = {
    if (events.size < th.levels.head.count)
      firstlevel(0, events.size, th.id, events, th.levels.head)
    else
      firstlevel(0, 0, th.id, events, th.levels.head)
  }

  /** a recursive sliding window implementation */
  private def firstlevel(start: Int, end: Int, id: String, events: List[InEvent], thLevel: ThresholdLevel): OutEvent = {
    println(":" + events.size + ":" + start + ":" + end)
    if (end == events.size)
      OutEvent(breached = false, id, level = 0, end - start, events(start).time, events(end - 1).time, events(end - 1).time - events(start).time)
    else if (end - start + 1 == thLevel.count && events(end).time <= events(start).time + thLevel.duration)
      OutEvent(breached = true, id, level = 0, end - start + 1, events(start).time, events(end).time, events(end).time - events(start).time)
    else if (events(end).time > events(start).time + thLevel.duration)
      firstlevel(start + 1, end + 1, id, events, thLevel)
    else
      firstlevel(start, end + 1, id, events, thLevel)
  }

  private def subsequentBreach(th: ThresholdDefinition, events: List[InEvent], thc: ThresholdControl): OutEvent = {
    val level = th.levels.slice(thc.breachLevel + 1, th.levels.size).indexWhere(x => {
      if (x.count <= events.size && events(x.count - 1).time <= thc.breachStart + x.duration) true else false
    })

    level match {
      case -1 => OutEvent(breached = false, th.id, thc.breachLevel + 1, events.size, events.head.time, events.last.time, events.last.time - thc.breachStart)
      case _ => OutEvent(breached = true, th.id, thc.breachLevel + level + 1, events.size, events.head.time, events.last.time, events.last.time - thc.breachStart)
    }
  }

  private def generateThresholdControl(th: ThresholdDefinition, thc: ThresholdControl, o: OutEvent, context: Context): ThresholdControl = {
    if (o.breached) {
      val breachStart = if (thc.breached) thc.breachStart else o.start
      ThresholdControl(th.id, breachStart, o.level, context.currentProcessingTime)
    } else
      thc
  }

}

object BreachIdentificationFunction {
  def apply(): BreachIdentificationFunction = new BreachIdentificationFunction
}
