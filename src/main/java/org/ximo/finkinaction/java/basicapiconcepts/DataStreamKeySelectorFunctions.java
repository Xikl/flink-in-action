package org.ximo.finkinaction.java.basicapiconcepts;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author xikl
 * @date 2019/8/4
 */
public class DataStreamKeySelectorFunctions {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream("localhost", 9999)
                .flatMap(getStringTuple2FlatMapFunction())
                // 这里需要替换为class
                .returns(WordCount.class)
                .keyBy((KeySelector<WordCount, String>) wc -> wc.word)
                .timeWindow(Time.seconds(5))
                .sum("count")
                // 设置并发数
                .setParallelism(1)
                .print();

        env.execute("DataStreamKeySelectorFunctions");
    }

    private static FlatMapFunction<String, WordCount> getStringTuple2FlatMapFunction() {
        return (String line, Collector<WordCount> collector) -> {
            final String[] wordArray = line.toLowerCase().split(",");
            Arrays.stream(wordArray)
                    .filter(StringUtils::isNotEmpty)
                    .forEach(word -> collector.collect(new WordCount(word, 1)));
        };
    }


    /**
     * 15:44:53,352 INFO  org.apache.flink.api.java.typeutils.TypeExtractor             - Class org.ximo.finkinaction.java.basicapiconcepts.DataStreamKeySelectorFunctions$WordCount is not public so it cannot be used as a POJO type and must be processed as GenericType. Please read the Flink documentation on "Data Types & Serialization" for details of the effect on performance.
     * Exception in thread "main" org.apache.flink.api.common.typeutils.CompositeType$InvalidFieldReferenceException: Cannot reference field by field expression on GenericType<org.ximo.finkinaction.java.basicapiconcepts.DataStreamKeySelectorFunctions.WordCount>Field expressions are only supported on POJO types, tuples, and case classes. (See the Flink documentation on what is considered a POJO.)
     * 	at org.apache.flink.streaming.util.typeutils.FieldAccessorFactory.getAccessor(FieldAccessorFactory.java:193)
     * 	at org.apache.flink.streaming.api.functions.aggregation.SumAggregator.<init>(SumAggregator.java:55)
     * 	at org.apache.flink.streaming.api.datastream.WindowedStream.sum(WindowedStream.java:1367)
     * 	at org.ximo.finkinaction.java.basicapiconcepts.DataStreamKeySelectorFunctions.main(DataStreamKeySelectorFunctions.java:30)
     * 	如果不是public的 他会报错
     *
     */
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class WordCount {
        private String word;

        private int count;
    }


}
