package org.ximo.finkinaction.scala.datastream.connectors

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper

/**
  * 将本地的nc 命令产生的数据写入到kafka
  *
  * @author xikl
  * @date 2019/10/9
  */
object KafkaConnectorProducerApp extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val data = env.socketTextStream("localhost", 9999)

  env.enableCheckpointing(5000)
  env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
  env.getCheckpointConfig.setCheckpointTimeout(10000)
  env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)


  val topic = "test"
  val properties = new Properties()
  properties.setProperty("bootstrap.servers", "47.98.200.49:9092")

  //  val stringFlinkKafkaProducer =
  //    new FlinkKafkaProducer[String](topic, new SimpleStringSchema(), properties)

  val stringFlinkKafkaProducer = new FlinkKafkaProducer[String](
    topic,
    new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()),
    properties,
    FlinkKafkaProducer.Semantic.EXACTLY_ONCE
  )

  data.addSink(stringFlinkKafkaProducer)

  env.execute("KafkaConnectorProducerApp-scala")

}
