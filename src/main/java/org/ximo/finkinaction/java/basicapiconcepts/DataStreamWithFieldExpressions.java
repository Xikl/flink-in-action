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
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream("localhost", 9999)
                .flatMap(getStringTuple2FlatMapFunction())
                // 这里需要替换为class
                .returns(Types.POJO(WordCount.class))
                .keyBy("word")
                .timeWindow(Time.seconds(5))
                .sum("count")
                // 设置并发数
                .setParallelism(1)
                .print();
    }

    private static FlatMapFunction<String, WordCount> getStringTuple2FlatMapFunction() {
        return (String line, Collector<WordCount> collector) -> {
            final String[] wordArray = line.toLowerCase().split(",");
            Arrays.stream(wordArray)
                    .filter(StringUtils::isNotEmpty)
                    .forEach(word -> collector.collect(new WordCount(word, 1)));
        };
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class WordCount {

        private String word;
        private int count;

    }
}

