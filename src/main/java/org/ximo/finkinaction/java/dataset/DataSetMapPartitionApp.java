package org.ximo.finkinaction.java.dataset;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * @author xikl
 * @date 2019/8/4
 */
public class DataSetMapPartitionApp {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<String> data = Stream.iterate(1, i -> i + 1)
                .map(Object::toString)
                .limit(100)
                .collect(toList());

        //34
        //33
        //33
        env.fromCollection(data)
                .setParallelism(3)
                .mapPartition(new PartitionCounter())
                .print();

        // 有点意思 首先lambda中要写明类型 否则无法正常编写
        env.fromCollection(data)
                .setParallelism(3)
                .mapPartition((Iterable<String> values, Collector<Long> out) -> {
                    long c = 0;
                    for (String s : values) {
                        c++;
                    }
                    out.collect(c);
                })
                .returns(Types.LONG)
                .print();

    }

    public static class PartitionCounter implements MapPartitionFunction<String, Long> {

        @Override
        public void mapPartition(Iterable<String> values, Collector<Long> out) {
            long c = 0;
            for (String s : values) {
                c++;
            }
            out.collect(c);
        }
    }
}
