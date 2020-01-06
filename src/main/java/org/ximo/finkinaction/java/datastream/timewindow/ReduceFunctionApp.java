package org.ximo.finkinaction.java.datastream.timewindow;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Arrays;

/**
 * @author xikl
 * @date 2019/9/10
 */
public class ReduceFunctionApp {

    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStreamSource<String> input = environment.socketTextStream("localhost", 9999);

        input.flatMap(toTuple2())
                .keyBy(t -> t.f0)
                .timeWindow(Time.seconds(10), Time.seconds(5))
                .reduce((ReduceFunction<Tuple2<String, Long>>) (value1, value2) ->
                        new Tuple2<>(value1.f0, value1.f1 + value2.f1))
                .print()
                .setParallelism(1);


    }

    private static FlatMapFunction<String, Tuple2<String, Long>> toTuple2() {
        return (line, collector) -> {
            final String[] wordArray = line.toLowerCase().split(",");
            Arrays.stream(wordArray)
                    .filter(StringUtils::isNotEmpty)
                    .forEach(word -> collector.collect(new Tuple2<>(word, 1L)));
        };
    }
}
