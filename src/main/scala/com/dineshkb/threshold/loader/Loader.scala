package com.dineshkb.threshold.loader

import com.dineshkb.threshold.domain.{ThresholdControl, ThresholdDefinition}

trait Loader {
  def init(): Unit

  def getDefinition(): Map[String, ThresholdDefinition]

  def getControl(): scala.collection.mutable.Map[String, ThresholdControl]
}

object Loader {

  @throws(classOf[Exception])
  def apply(name: String): Loader = {
    name match {
      case "file" => val f = new FileLoader; f.init(); f
      case _ => throw new Exception(name + " related class not found")
    }
  }
}
