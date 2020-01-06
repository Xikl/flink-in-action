package org.ximo.flinkincation.java.dataset;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.junit.Test;

/**
 * @author xikl
 * @date 2019/12/23
 */
public class DataStreamTest {

    /**
     *
     * @throws Exception
     * @see StreamExecutionEnvironment#generateSequence(long, long)
     * @see StreamExecutionEnvironment#readTextFile(String)
     * @see StreamExecutionEnvironment#socketTextStream(String, int)
     */
    @Test
    public void testWordCount() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // dataStream中为keyBy dataSet中为groupBy
        DataStream<Tuple2<String, Integer>> dataStream = env
                .socketTextStream("localhost", 9999)
                .flatMap(new Splitter())
                .keyBy(t -> t.f0)
                .timeWindow(Time.seconds(5))
                .sum(1);

        dataStream.print();

        // dataStream 中必写
        env.execute("Window WordCount");
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) {
            for (String word: sentence.split(" ")) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }

    @Test
    public void testCoGroup() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final FlatMapFunction<String, Tuple2<Integer, String>> flatMapFunction = (String data, Collector<Tuple2<Integer, String>> collector) -> {
            final String[] result = StringUtils.split(data, " ");
            collector.collect(new Tuple2<>(Integer.valueOf(result[0]), result[1]));
        };

        DataStream<Tuple2<Integer, String>> inputStream1 = env.socketTextStream("localhost", 9999)
                .flatMap(flatMapFunction)
                .returns(Types.TUPLE(Types.INT, Types.STRING));

        DataStream<Tuple2<Integer, String>> inputStream2 = env.socketTextStream("localhost", 9998)
                .flatMap(flatMapFunction)
                .returns(Types.TUPLE(Types.INT, Types.STRING));

        // 没有关联上的也会输出
        inputStream1.coGroup(inputStream2)
                .where(leftData -> leftData.f0).equalTo(rightData -> rightData.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new CoGroupFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Object>() {
                    @Override
                    public void coGroup(Iterable<Tuple2<Integer, String>> first,
                                        Iterable<Tuple2<Integer, String>> second,
                                        Collector<Object> out) throws Exception {
                        System.out.println(first);
                        System.out.println(second);
                    }
                    // todo.assignTimestampsAndWatermarks()
                });

        env.execute();
    }

    @Test
    public void testDataStreamCloseWithIteration() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Long> someIntegers = env.generateSequence(0, 1000);

        IterativeStream<Long> iteration = someIntegers.iterate();

        DataStream<Long> minusOne = iteration.map(value -> value - 1);

        DataStream<Long> stillGreaterThanZero = minusOne.filter(value -> value > 0);

        iteration.closeWith(stillGreaterThanZero);

        iteration.print().setParallelism(1);
//        DataStream<Long> lessThanZero = minusOne.filter(value -> value <= 0);

        env.execute();
    }

    @Test
    public void testEventTime() {

        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        streamExecutionEnvironment.socketTextStream("localhost", 9999)
                .flatMap(new Splitter())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<String, Integer>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple2<String, Integer> element) {
                        return 0;
                    }
                })
                .keyBy(0)
                // 滚动窗口 如果想要使用 滑动 请添加 slide
//                .timeWindow(Time.seconds(10))
                .timeWindow(Time.seconds(10), Time.seconds(5))
                .sum(1)
                .print()
                .setParallelism(1);
    }
}
