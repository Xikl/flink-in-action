package org.ximo.finkinaction.java.dataset;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * @author xikl
 * @date 2019/8/7
 */
public class DataSetFlatMapApp {


    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        List<String> data = Stream.of("data,java", "scala,flink,java,ptyhon", "data,sprk").collect(toList());

        // 及其繁琐 flatMap
        env.fromCollection(data)
                .flatMap((String value, Collector<String> out) -> {
                    Arrays.stream(value.split(","))
                            .forEach(out::collect);
                }).returns(Types.STRING)
                .map(item -> new Tuple2<>(item, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .groupBy(0)
                .sum(1)
                .print();

        // flatMap直接进行Tuple2转化
        env.fromCollection(data)
                .flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
                    Arrays.stream(value.split(","))
                            .forEach(item -> out.collect(new Tuple2<>(item, 1)));
                }).returns(Types.TUPLE(Types.STRING, Types.INT))
                .groupBy(0)
                .sum(1)
                .print();

    }
}
