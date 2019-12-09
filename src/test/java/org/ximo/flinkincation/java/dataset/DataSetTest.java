package org.ximo.flinkincation.java.dataset;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.junit.Test;

/**
 * @author xikl
 * @date 2019/12/9
 */
public class DataSetTest {

    @Test
    public void testDataSetTransformations() throws Exception {
        // 拿到执行上下文
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 初始数据
        DataSet<String> text = env.fromElements(
                "Who's there?",
                "I think I hear them. Stand, ho! Who's there?");

        // 转化
        DataSet<Tuple2<String, Integer>> wordCounts = text
                .flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
                    for (String word : line.split(" ")) {
                        out.collect(new Tuple2<>(word, 1));
                    }
                })
                // 不加return type 会报错
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .map(tuple -> new Tuple2<>(tuple.f0.toUpperCase(), tuple.f1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .filter(tuple -> !tuple.f0.contains("?"))
                .groupBy(0)
                .sum(1)
                .first(5)
                // 多个排序 用链式表达
                // Locally sorts all partitions of a data 可能会 oom
                .sortPartition(0, Order.ASCENDING)
                .sortPartition(1, Order.DESCENDING);

        // 简单输出
        wordCounts.print();
    }

}
