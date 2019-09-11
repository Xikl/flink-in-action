package org.ximo.finkinaction.java.datastream.timewindow;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author xikl
 * @date 2019/9/12
 */
public class AggregateFunctionApp {

    public static void main(String[] args) {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStreamSource<String> input = environment.socketTextStream("localhost", 9999);
        input.flatMap(toTuple2())
                .keyBy(0)
                .timeWindow(Time.seconds(10), Time.seconds(5))
                .aggregate(new AverageAggregate())
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

    /**
     * 求平均值
     */
    private static class AverageAggregate
            implements AggregateFunction<Tuple2<String, Long>, Tuple2<Long, Long>, Double> {
        // 创建一个初始值
        @Override
        public Tuple2<Long, Long> createAccumulator() {
            return new Tuple2<>(0L, 0L);
        }

        @Override
        public Tuple2<Long, Long> add(Tuple2<String, Long> value,
                                      Tuple2<Long, Long> accumulator) {
            return new Tuple2<>(value._2 + accumulator._1, accumulator._2 + 1L);
        }

        @Override
        public Double getResult(Tuple2<Long, Long> accumulator) {
            return ((double) accumulator._1) / accumulator._2;
        }

        @Override
        public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
            return new Tuple2<>(a._1 + b._1, a._2 / b._2);
        }
    }


}
