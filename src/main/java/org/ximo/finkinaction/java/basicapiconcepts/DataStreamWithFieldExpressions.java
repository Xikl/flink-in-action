package org.ximo.finkinaction.java.basicapiconcepts;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author xikl
 * @date 2019/8/4
 */
public class DataStreamWithFieldExpressions {

    /**
     * 这里我觉得有点弱
     * 它不支持java8的方法引用
     * java中tuple KeyBy("f0") 他是从0开始的 scala 则是 _1 从1开始这里要注意一下
     * 多个对象可以用
     * "count": The count field in the WC class.
     *
     * "complex": Recursively selects all fields of the field complex of POJO type ComplexNestedClass.
     *
     * "complex.word.f2": Selects the last field of the nested Tuple3.
     *
     * "complex.hadoopCitizen": Selects the Hadoop IntWritable type.
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream("localhost", 9999)
                .flatMap(getStringTuple2FlatMapFunction())
                // 这里需要替换为class
                .returns(WordCount.class)
                .keyBy("word")
                .timeWindow(Time.seconds(5))
                .sum("count")
                // 设置并发数
                .setParallelism(1)
                .print();

        env.execute("DataStreamWithFieldExpressions");
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
     * 15:47:26,646 INFO  org.apache.flink.api.java.typeutils.TypeExtractor             - Class org.ximo.finkinaction.java.basicapiconcepts.DataStreamWithFieldExpressions$WordCount is not public so it cannot be used as a POJO type and must be processed as GenericType. Please read the Flink documentation on "Data Types & Serialization" for details of the effect on performance.
     * Exception in thread "main" org.apache.flink.api.common.InvalidProgramException: This type (GenericType<org.ximo.finkinaction.java.basicapiconcepts.DataStreamWithFieldExpressions.WordCount>) cannot be used as key.
     * 	at org.apache.flink.api.common.operators.Keys$ExpressionKeys.<init>(Keys.java:330)
     * 	at org.apache.flink.streaming.api.datastream.DataStream.keyBy(DataStream.java:337)
     * 	at org.ximo.finkinaction.java.basicapiconcepts.DataStreamWithFieldExpressions.main(DataStreamWithFieldExpressions.java:42)
     *
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WordCount {

        private String word;
        private int count;

    }
}

