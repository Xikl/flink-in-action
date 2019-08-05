package org.ximo.finkinaction.java.dataset;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * @author xikl
 * @date 2019/8/5
 */
public class DataSetFirstNApp {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        List<Tuple2<Integer, String>> data = Stream.of(
                new Tuple2<>(1, "scala"),
                new Tuple2<>(1, "java"),
                new Tuple2<>(1, "python"),
                new Tuple2<>(2, "flink"),
                new Tuple2<>(2, "saprk"),
                new Tuple2<>(3, "netty"),
                new Tuple2<>(4, "vue")
                ).collect(toList());

        env.fromCollection(data)
                .groupBy(0)
                .first(3)
                .print();

        env.fromCollection(data)
                .groupBy(0)
                .sortGroup(1, Order.ASCENDING)
                .first(3)
                .print();
    }
}
