package org.ximo.finkinaction.java.datastream.connectors;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;

import java.util.Properties;

/**
 * 生产者
 *
 * @author xikl
 * @date 2019/10/8
 */
public class KafkaConnectorProducerApp {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 检查点 配置
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // 从 socket接受数据 然后写入到kafka
        final DataStreamSource<String> data = env.socketTextStream("localhost", 9999);

        String topic = "test";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "47.98.200.49:9092");

//        final FlinkKafkaProducer<String> stringFlinkKafkaProducer =
//                new FlinkKafkaProducer<>(topic, new SimpleStringSchema(), properties);

        // 精确一次
        final FlinkKafkaProducer<String> stringFlinkKafkaProducer =
                new FlinkKafkaProducer<>(topic,
                        new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()),
                        properties,
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

//        stringFlinkKafkaProducer.setWriteTimestampToKafka(true);

        data.addSink(stringFlinkKafkaProducer);
        env.execute("KafkaConnectorProducerApp-java");

    }

}
