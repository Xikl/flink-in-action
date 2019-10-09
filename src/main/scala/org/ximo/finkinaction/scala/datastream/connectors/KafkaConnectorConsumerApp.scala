package org.ximo.finkinaction.scala.datastream.connectors

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.scala._

/**
  *
  *
  * @author xikl
  * @date 2019/10/9
  */
object KafkaConnectorConsumerApp extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val properties = new Properties()
  properties.setProperty("bootstrap.servers", "47.98.200.49:9092")
  properties.setProperty("group.id", "test")

  val topic = "test"

  val flinkKafkaConsumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)

  val value: DataStream[String] = env.addSource(flinkKafkaConsumer)
  value.print()

  env.execute("KafkaConnectorConsumerApp-scala")



}
