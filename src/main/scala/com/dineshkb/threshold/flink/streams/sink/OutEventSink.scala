package com.dineshkb.threshold.flink.streams.sink

import java.util.Properties

import com.dineshkb.threshold.domain.OutEvent
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants

trait OutEventSink extends SinkFunction[OutEvent]

object OutEventSink {
  @throws(classOf[Exception])
  def apply(name: String): OutEventSink =
    name match {
      //kinesis needs special handling as it has no no-args constructor
      case "com.dineshkb.threshold.flink.streams.sink.OutEventKinesisSink" => {
        val config = new Properties()
        config.put(AWSConfigConstants.AWS_REGION, System.getProperty("kinesis.aws.region"))
        config.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "kinesis.aws.accessKey")
        config.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "kinesis.aws.secretAccessKey")

        val k = new OutEventKinesisSink(config)
        k.setFailOnError(true)
        k.setDefaultStream(System.getProperty("sink.kinesis.streamName"))
        k.setDefaultPartition("0")

        k
      }
      case _ => Class.forName(name).getConstructor().newInstance().asInstanceOf[OutEventSink]
    }
}