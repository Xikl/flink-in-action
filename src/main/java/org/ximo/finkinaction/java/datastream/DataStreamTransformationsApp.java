package org.ximo.finkinaction.java.datastream;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * @author xikl
 * @date 2019/9/2
 */
public class DataStreamTransformationsApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<Integer> list = Stream.iterate(0, i -> i + 1).limit(100).collect(toList());

        env.fromCollection(list)
                .map(i -> i * 2)
                .filter(i -> i % 2 == 0)
                .print()
                .setParallelism(1);

        env.execute();

    }

}
