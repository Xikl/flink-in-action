package org.ximo.finkinaction.java.datastream.connectors;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;

/**
 * @author xikl
 * @date 2019/9/23
 */
public class FileSink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStreamSource<String> input = env.socketTextStream("localhost", 9999);

        input.print().setParallelism(1);

        String filePath = "D:\\log\\flink\\filesink";
        BucketingSink<String> sink = new BucketingSink<>(filePath);

        sink.setBucketer(new DateTimeBucketer<>("yyyy-MM-dd--HHmm"));
        sink.setWriter(new StringWriter<>());
//        sink.setBatchSize(1024 * 1024 * 400);
        // this is 20 s
        sink.setBatchRolloverInterval(20);

        input.addSink(sink);

        env.execute("FileSink");
    }



}
