package org.ximo.finkinaction.java.datastream;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/datastream_api.html#data-sources
 *
 * @author xikl
 * @date 2019/8/29
 */
public class DataSourceApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStreamSource<String> input = env.socketTextStream("localhost", 9999);
        input.print().setParallelism(1);

        env.execute();
    }
}
