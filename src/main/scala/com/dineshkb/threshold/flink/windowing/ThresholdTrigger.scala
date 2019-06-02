package com.dineshkb.threshold.flink.windowing

import com.dineshkb.threshold
import com.dineshkb.threshold.domain.{EnrichedEvent, ThresholdControl, ThresholdDefinition}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.Window

class ThresholdTrigger extends Trigger[EnrichedEvent, Window] {

  val controlDesc = new ValueStateDescriptor[ThresholdControl]("control", TypeInformation.of(new TypeHint[ThresholdControl] {}))
  val syncAchievedDesc = new ValueStateDescriptor[Boolean]("syncAchievedDesc", TypeInformation.of(new TypeHint[Boolean] {}))

  override def onElement(e: EnrichedEvent, timestamp: Long, window: Window, ctx: Trigger.TriggerContext): TriggerResult = {

    println("T" + e.thDef.id + ":" + ctx.getCurrentWatermark + ":" + Thread.currentThread().getId())

    val th = e.thDef
    val thc = getUpdatedThresholdControlState(e.thCtrl, ctx)

    th match {
      case threshold.UNDEFINED => TriggerResult.PURGE
      case _ =>
        if (thc.breached) {
          if (furtherBreachPossible(ctx.getCurrentWatermark, th, thc)) { //continue if further breaches are possible else purge
            if (e.inEvent.time >= ctx.getCurrentWatermark) //register event only if it is not late
              th.levels.slice(thc.breachLevel + 1, th.levels.size)
                .foreach(x => ctx.registerEventTimeTimer(thc.breachStart + x.duration))
            TriggerResult.CONTINUE
          }
          else
            TriggerResult.PURGE
        } else {
          if (e.inEvent.time >= ctx.getCurrentWatermark) //register event only if it is not late
            ctx.registerEventTimeTimer(e.inEvent.time + th.levels.head.duration)
          TriggerResult.CONTINUE
        }
    }
  }

  private def furtherBreachPossible(time: Long, th: ThresholdDefinition, thc: ThresholdControl): Boolean =
    thc.breachStart + th.levels.last.duration >= time

  /*This method considers the below 7 input conditions and updates the threshold control state
    Input						                          Output
    Event		      State		      SyncAchieved	State(cs)     SyncAchieved(ss)
    Not Breached	Null		      FALSE		      Not Breached	FALSE (This is the usual condition on start up)
    Not Breached	Not Breached	FALSE		      Not Breached	FALSE (no breach has occurred)
    Not Breached	Breached	    FALSE		      Breached	    FALSE (breach has occurred, however cache has not synchronized)
    Breached	    Breached	    FALSE		      Breached	    TRUE  (breach has occurred, cache has just synchronized)
    Breached	    Breached	    TRUE		      Breached	    TRUE  (breach has occurred, cache has synchronized)
    Not Breached	Breached	    TRUE		      Not Breached	FALSE (breach that occurred, is now closed)
    Breached	    Null		      FALSE		      Breached	    TRUE  (Condition when the job is re-started mid-breach)

    Breached	    Null		      TRUE		      Input not feasible, SyncAchieved is never TRUE when State is null
    Not Breached	Not Breached	TRUE		      Input not feasible, SyncAchieved is never TRUE when State is Not Breached
    Breached	    Not Breached	FALSE		      Input not feasible, State is never Not Breached when Event is Breached
    Breached	    Not Breached	TRUE		      Input not feasible, State is never Not Breached when Event is Breached
    Not Breached	Null		      TRUE		      Input not feasible, SyncAchieved is never TRUE when State is null
   */
  private def getUpdatedThresholdControlState(thCtrlEvent: ThresholdControl, ctx: Trigger.TriggerContext): ThresholdControl = {

    val cs = ctx.getPartitionedState(controlDesc)
    val ss = ctx.getPartitionedState(syncAchievedDesc)

    val thCtrlState = cs.value
    val syncAchieved = ss.value

    cs.update(
      if (thCtrlState == null || thCtrlState.breached && !thCtrlEvent.breached && syncAchieved)
        thCtrlEvent
      else
        thCtrlState
    )

    ss.update(
      if (!thCtrlEvent.breached && syncAchieved && thCtrlState.breached)
        false
      else if (thCtrlEvent.breached && thCtrlState.breached)
        true
      else
        syncAchieved
    )

    cs.value
  }

  override def onProcessingTime(time: Long, window: Window, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onEventTime(time: Long, window: Window, ctx: Trigger.TriggerContext): TriggerResult = {
    println("On" + ctx.getCurrentWatermark + ":" + time)

    ctx.deleteEventTimeTimer(time)
    TriggerResult.FIRE_AND_PURGE
  }

  override def clear(window: Window, ctx: Trigger.TriggerContext): Unit = {}
}

object ThresholdTrigger {
  def apply(): ThresholdTrigger = new ThresholdTrigger
}