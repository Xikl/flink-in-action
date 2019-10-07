package org.ximo.finkinaction.java.datastream.connectors;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author xikl
 * @date 2019/10/7
 */
public class KafkaConnectorConsumerApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        // todo 改为阿里云的ip
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");

        String topic = "test";
        FlinkKafkaConsumer<String> flinkKafkaConsumer =
                new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);

        final DataStreamSource<String> stringDataStreamSource = env.addSource(flinkKafkaConsumer);
        env.execute("KafkaConnectorConsumerApp-Java");

    }

}
