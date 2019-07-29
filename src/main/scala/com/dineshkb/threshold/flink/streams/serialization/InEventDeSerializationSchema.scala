package com.dineshkb.threshold.flink.streams.serialization

import com.dineshkb.threshold.domain.InEvent
import net.liftweb.json._
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation

class InEventDeSerializationSchema extends DeserializationSchema[InEvent] {

  override def deserialize(message: Array[Byte]): InEvent = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    val json = parse(new String(message, java.nio.charset.StandardCharsets.UTF_8))
    json.extract[InEvent]
  }

  override def isEndOfStream(nextElement: InEvent): Boolean = false

  override def getProducedType: TypeInformation[InEvent] = TypeInformation.of(classOf[InEvent])
}
