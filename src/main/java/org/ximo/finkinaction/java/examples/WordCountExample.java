package org.ximo.finkinaction.java.examples;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Collection;

/**
 * @author xikl
 * @date 2019/12/5
 */
public class WordCountExample {

    /**
     * @param args 参数
     * @throws Exception e
     * @see ExecutionEnvironment#fromCollection(Collection)
     * @see ExecutionEnvironment#fromElements(Object[])
     * @see ExecutionEnvironment#readCsvFile(String)
     * @see ExecutionEnvironment#readTextFile(String)
     * @see ExecutionEnvironment#readTextFileWithValue(String)
     *        特殊的{@link org.apache.flink.types.StringValue} 类似StringBuilder等可变的字符串{@code mutable}
     */
    public static void main(String[] args) throws Exception {
        // 拿到执行上下文
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 初始数据
        DataSet<String> text = env.fromElements(
                "Who's there?",
                "I think I hear them. Stand, ho! Who's there?");

        // 转化
        // 特殊的flatMap --> Java 版本中
        DataSet<Tuple2<String, Integer>> wordCounts = text
                .flatMap(new LineSplitter())
                .groupBy(0)
                .sum(1);

        // 简单输出
        wordCounts.print();
    }

    public static class LineSplitter
            implements FlatMapFunction<String, Tuple2<String, Integer>> {

        private static final String LINE_SPLIT = " ";

        /**
         * 解析整个字符串 输出一个 key value 的结构 如 {@code (hear, 1)}
         * @param line 整个字符串
         * @param out 一个{@link Collector}收集器
         */
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
            for (String word : line.split(LINE_SPLIT)) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}