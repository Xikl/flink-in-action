package org.ximo.finkinaction.java.datastream.connectors;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * 连接阿里云的kafka解决
 * host.name=阿里云内网地址      #kafka绑定的interface
 * advertised.listeners=PLAINTEXT://阿里云外网映射地址:9092
 *
 * @author xikl
 * @date 2019/10/7
 */
public class KafkaConnectorConsumerApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "47.98.200.49:9092");
        properties.setProperty("group.id", "test");

        String topic = "test";
        FlinkKafkaConsumer<String> flinkKafkaConsumer =
                new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);

        final DataStreamSource<String> stringDataStreamSource = env.addSource(flinkKafkaConsumer);
        stringDataStreamSource.print();
        env.execute("KafkaConnectorConsumerApp-Java");

    }

}
