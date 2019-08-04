package org.ximo.finkinaction.java.examples;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author xikl
 * @date 2019/8/4
 */
public class StreamingWordCountWithJava {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.socketTextStream("localhost", 9999)
                .flatMap(getStringTuple2FlatMapFunction())
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
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
