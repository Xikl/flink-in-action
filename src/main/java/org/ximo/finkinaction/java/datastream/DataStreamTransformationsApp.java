package org.ximo.finkinaction.java.datastream;

import org.apache.flink.streaming.api.datastream.SplitStream;
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

//        env.fromCollection(list)
//                .map(i -> i * 2)
//                .filter(i -> i % 2 == 0)
//                .print()
//                .setParallelism(1);
//
//        // union
//        env.fromElements(1, 2).union(env.fromElements(3, 4))
//                .print();

        final SplitStream<Integer> split = env.fromCollection(list)
                .split(num -> {
                    if (num % 2 == 0) {
                        return Stream.of("even").collect(toList());
                    } else {
                        return Stream.of("odd").collect(toList());
                    }
                });

        split.select("odd").print().setParallelism(1);

        env.execute();

    }

}
