package com.dineshkb.threshold.flink.streams.source

import java.util.Properties

import com.dineshkb.threshold.domain.InEvent
import com.dineshkb.threshold.flink.serialization.InEventDeSerializationSchema
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer

class InEventKinesisSource(stream: String, schema : DeserializationSchema[InEvent], config: Properties) extends FlinkKinesisConsumer[InEvent](stream, schema, config) with InEventSource {
  def this(stream: String, config: Properties) {
    this(stream, new InEventDeSerializationSchema, config)
  }
}
