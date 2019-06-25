package com.dineshkb.threshold.flink.serialization

import com.dineshkb.threshold.domain.OutEvent
import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write
import org.apache.flink.api.common.serialization.SerializationSchema

class OutEventSerializationSchema extends SerializationSchema[OutEvent] {
  override def serialize(element: OutEvent): Array[Byte] = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    write(element).getBytes(java.nio.charset.StandardCharsets.UTF_8)
  }
}
