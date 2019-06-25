package com.dineshkb.threshold.flink.streams.sink

import java.util.Properties

import com.dineshkb.threshold.domain.OutEvent
import com.dineshkb.threshold.flink.serialization.OutEventSerializationSchema
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer

class OutEventKinesisSink(schema: SerializationSchema[OutEvent], config: Properties) extends FlinkKinesisProducer[OutEvent](schema, config) with OutEventSink {
  def this(config: Properties) {
    this(new OutEventSerializationSchema, config)
  }
}
