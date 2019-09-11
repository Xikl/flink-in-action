package org.ximo.finkinaction.java.datastream.timewindow;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.util.Arrays;

/**
 * ProcessWindowFunction中的keyby必须要指定key的类型
 * 该key参数是通过KeySelector为keyBy()调用指定的密钥提取的密钥。在元组索引键或字符串字段引用的情况下，此键类型始终是Tuple，您必须手动将其转换为正确大小的元组以提取键字段。
 *
 * @author xikl
 * @date 2019/9/12
 */
public class ProcessWindowFunctionApp {

    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStreamSource<String> input = environment.socketTextStream("localhost", 9999);
        input.flatMap(toTuple2())
                // 该key参数是通过KeySelector为keyBy()调用指定的密钥提取的密钥。在元组索引键或字符串字段引用的情况下，
                // 此键类型始终是Tuple，您必须手动将其转换为正确大小的元组以提取键字段。
                .keyBy(t -> t._1)
                .timeWindow(Time.seconds(10), Time.seconds(5))
                .process(new CountWindowFunction())
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
     * ProcessWindowFunction获取包含窗口所有元素的Iterable，
     * 以及可访问时间和状态信息的Context对象，
     * 这使其能够提供比其他窗口函数更多的灵活性。
     * 这是以性能和资源消耗为代价的，
     * 因为元素不能以递增方式聚合，而是需要在内部进行缓冲，直到认为窗口已准备好进行处理。
     *
     */
    public static class CountWindowFunction
            extends ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow> {

        @Override
        public void process(String key,
                            Context context,
                            Iterable<Tuple2<String, Long>> elements,
                            Collector<String> out) throws Exception {
            // key是keyBy对应的那个参数
            // processWindowFunction 会缓冲当前窗口的所有的数据
            long count = 0;
            for (Tuple2<String, Long> in: elements) {
                count++;
            }
            out.collect("Window: " + context.window() + "count: " + count);
        }
    }
}
