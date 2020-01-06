package org.ximo.finkinaction.java.datastream.timewindow;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Arrays;

/**
 * @author xikl
 * @date 2019/9/10
 */
public class TumblingEventTimeWindowsApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStreamSource<String> input = env.socketTextStream("localhost", 9999);

        input.flatMap(getStringTuple2FlatMapFunction())
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(t -> t.f0)
                // 默认按照处理时间processing 这是它的简写
//                .timeWindow(Time.seconds(5))
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1)
                .print()
                .setParallelism(1);

        env.execute();
    }

    /**
     * 多练吧
     * 这个flatMap和java自带的还不一样 他是 collector.collect的void的类型
     *
     * @return
     */
    private static FlatMapFunction<String, Tuple2<String, Integer>> getStringTuple2FlatMapFunction() {
        return (line, collector) -> {
            final String[] wordArray = line.toLowerCase().split(",");
            Arrays.stream(wordArray)
                    .filter(StringUtils::isNotEmpty)
                    .forEach(word -> collector.collect(new Tuple2<>(word, 1)));
        };
    }
}
