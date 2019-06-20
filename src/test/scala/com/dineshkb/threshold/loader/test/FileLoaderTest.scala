package com.dineshkb.threshold.loader.test

import com.dineshkb.threshold.loader._
import net.liftweb.json._
import org.scalatest.{FlatSpec, Matchers, Outcome}

class FileLoaderTest extends FlatSpec with Matchers {

  implicit val formats: DefaultFormats.type = DefaultFormats

  val loader = Loader("com.dineshkb.threshold.loader.stub.FileLoader")

  override def withFixture(test: NoArgTest): Outcome = {
    setProperties()

    loader.open()
    try super.withFixture(test)
    finally {
      loader.close()
    }
  }

  def setProperties(): Unit = {
    System.setProperty("threshold.loader.file.definition", """.\src\test\resources\fileloader\definition.json""")
    System.setProperty("threshold.loader.file.control", """.\src\test\resources\fileloader\control.json""")
  }

  behavior of "File loader class"

  it should "output the two thresholds defined within the input json file" in {

    val m = loader.getDefinition()
    println(m)

    m.size shouldEqual 2
  }

  it should "output the two threshold controls defined within the input json file" in {

    val m = loader.getControl()
    println(m)

    m.size shouldEqual 2
  }
}
