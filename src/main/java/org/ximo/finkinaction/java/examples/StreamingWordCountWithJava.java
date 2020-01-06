package org.ximo.finkinaction.java.examples;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author xikl
 * @date 2019/8/4
 */
public class StreamingWordCountWithJava {

    /**
     * Returns {@link ParameterTool} for the given arguments. The arguments are keys followed by values.
     * Keys have to start with '-' or '--'
     *
     * <p><strong>Example arguments:</strong>
     * --key1 value1 --key2 value2 -key3 value3
     *
     * @param args Input array arguments
     * @return A {@link ParameterTool}
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 可以根据
        int port;
        try {
            // idea运行 中 添加 --port 9999 or -port 9999
            final ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        } catch (Exception e) {
            port = 9999;
            System.err.println("使用默认端口： 9999");
        }

        env.socketTextStream("localhost", port)
                .flatMap(getStringTuple2FlatMapFunction())
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(t -> t.f0)
                .timeWindow(Time.seconds(5))
                .sum(1)
                // 设置并发数
                .setParallelism(1)
                .print();

        // 必须要执行方法
        env.execute("StreamingWordCountWithJava");
    }

    private static FlatMapFunction<String, Tuple2<String, Integer>> getStringTuple2FlatMapFunction() {
        return (String line, Collector<Tuple2<String, Integer>> collector) -> {
            final String[] wordArray = line.toLowerCase().split(",");
            Arrays.stream(wordArray)
                    .filter(StringUtils::isNotEmpty)
                    .forEach(word -> collector.collect(new Tuple2<>(word, 1)));
        };
    }

}
