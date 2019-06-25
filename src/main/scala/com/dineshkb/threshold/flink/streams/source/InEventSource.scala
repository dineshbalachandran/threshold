package com.dineshkb.threshold.flink.streams.source

import java.util.Properties

import com.dineshkb.threshold.domain.InEvent
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.connectors.kinesis.config.{AWSConfigConstants, ConsumerConfigConstants}

trait InEventSource extends SourceFunction[InEvent]

object InEventSource {

  @throws(classOf[Exception])
  def apply(name: String): InEventSource = name match {
    //kinesis needs special handling as it has no no-args constructor
    case "com.dineshkb.threshold.flink.streams.source.InEventKinesisSource" => {
      val config = new Properties()
      config.put(AWSConfigConstants.AWS_REGION, System.getProperty("kinesis.aws.region"))
      config.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "kinesis.aws.accessKey")
      config.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "kinesis.aws.secretAccessKey")
      config.put(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "source.kinesis.streamInitialPosition")

      new InEventKinesisSource(System.getProperty("source.kinesis.streamName"), config)
    }
    case _ => Class.forName(name).getConstructor().newInstance().asInstanceOf[InEventSource]
  }
}