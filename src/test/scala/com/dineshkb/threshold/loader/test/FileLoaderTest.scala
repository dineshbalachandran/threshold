package com.dineshkb.threshold.loader.test

import com.dineshkb.threshold.loader._
import net.liftweb.json._
import org.scalatest.{FlatSpec, Matchers}

class FileLoaderTest extends FlatSpec with Matchers {

  implicit val formats: DefaultFormats.type = DefaultFormats

  behavior of "File loader class"

  it should "output the two thresholds defined within the input json file" in {
    System.setProperty("threshold.loader.file.definition", """.\src\test\resources\fileloader\definition.json""")
    val m = Loader("file").getDefinition()
    println(m)

    m.size shouldEqual 2
  }

  it should "output the two threshold controls defined within the input json file" in {
    System.setProperty("threshold.loader.file.control", """.\src\test\resources\fileloader\control.json""")
    val m = Loader("file").getControl()
    println(m)

    m.size shouldEqual 2
  }
}
