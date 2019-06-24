package com.dineshkb.threshold.loader.stub

import com.dineshkb.threshold.domain.{ThresholdControl, ThresholdDefinition}
import com.dineshkb.threshold.loader.Loader
import net.liftweb.json._

import scala.io.Source

class FileLoader extends Loader {

  @transient private var definitionfile: String = _
  @transient private var controlfile: String = _

  override def getDefinition(): Map[String, ThresholdDefinition] = {

    implicit val formats: DefaultFormats.type = DefaultFormats
    val reader = Source.fromFile(definitionfile)
    val json = parse(reader.getLines.mkString)
    reader.close

    val elements = (json \\ "threshold").children
    elements.foldLeft(scala.collection.mutable.Map[String, ThresholdDefinition]())((m, element) => {
      val x = element.extract[ThresholdDefinition]
      m += (x.id -> x)
    }).toMap
  }

  override def getControl(): scala.collection.mutable.Map[String, ThresholdControl] = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    val reader = Source.fromFile(controlfile)
    val json = parse(reader.getLines.mkString)
    reader.close

    val elements = (json \\ "thresholdcontrol").children
    elements.foldLeft(scala.collection.mutable.Map[String, ThresholdControl]())((m, element) => {
      val x = element.extract[ThresholdControl]
      m += (x.id -> x)
    })
  }

  override def open(): Unit = {
    definitionfile = System.getProperty("threshold.loader.file.definition")
    controlfile = System.getProperty("threshold.loader.file.control")
  }

  override def close(): Unit = {}
}
