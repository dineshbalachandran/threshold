package com.dineshkb.threshold.loader

import com.dineshkb.threshold.domain.{ThresholdControl, ThresholdDefinition}

trait Loader {
  def open(): Unit
  def getDefinition(): Map[String, ThresholdDefinition]
  def getControl(): scala.collection.mutable.Map[String, ThresholdControl]

  def close(): Unit
}

object Loader {

  @throws(classOf[Exception])
  def apply(name: String): Loader = Class.forName(name).getConstructor().newInstance().asInstanceOf[com.dineshkb.threshold.loader.Loader]
}
