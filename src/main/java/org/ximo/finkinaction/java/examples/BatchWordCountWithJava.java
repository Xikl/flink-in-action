package org.ximo.finkinaction.java.examples;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * @author xikl
 * @date 2019/8/4
 */
public class BatchWordCountWithJava {

    /**
     * (in,3)
     * (java,1)
     * (flink,1)
     * (scala,2)
     * (action,3)
     * (hello,1)
     * 如果不写returns 那么将会报错 https://ci.apache.org/projects/flink/flink-docs-release-1.8/dev/java_lambdas.html#examples-and-limitations
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 绝对路径
//        String input = "file:///D:\\App\\Jetbrains\\IDEA\\WorkSpaces\\study\\flink-in-action\\input.txt";
        // 相对路径
        String input = "input.txt";
        final DataSource<String> text = env.readTextFile(input, StandardCharsets.UTF_8.name());
        // 打印一下
        text.print();

        // 用java8来替换官网的教程
        // 这写出来的代码实在是太奇怪了 强制要写return类型 Tuple的更加繁琐 不写匿名内部类的代价
        // 如果不写returns 那么将会报错 https://ci.apache.org/projects/flink/flink-docs-release-1.8/dev/java_lambdas.html#examples-and-limitations
        text.flatMap(getStringTuple2FlatMapFunction())
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .groupBy(0)
                .sum(1)
                .print();

    }

    private static FlatMapFunction<String, Tuple2<String, Integer>> getStringTuple2FlatMapFunction() {
        return (String line, Collector<Tuple2<String, Integer>> collector) -> {
            final String[] wordArray = line.toLowerCase().split(" ");
            Arrays.stream(wordArray)
                    .filter(StringUtils::isNotEmpty)
                    .forEach(word -> collector.collect(new Tuple2<>(word, 1)));
        };
    }


}
