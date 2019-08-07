package org.ximo.finkinaction.java.dataset;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import scala.Int;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * @author xikl
 * @date 2019/8/7
 */
public class DataSetDistinctApp {


    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        List<Integer> data1 = Stream.of(1, -2, 2, 4, -4, 5, -5).collect(toList());
        env.fromCollection(data1)
                .distinct()
                .print();


        List<Tuple3<String, Integer, String>> data2 = Stream.of(
                new Tuple3<>("a", 1, "b"),
                new Tuple3<>("a", 2, "b"),
                new Tuple3<>("a", 1, "b")
        ).collect(toList());
        env.fromCollection(data2)
                .distinct(1, 2)
                .print();

        env.fromCollection(data1)
                .distinct(Math::abs)
                .print();

        List<CustomType> data3 = Stream.of(
            new CustomType("aa", 2),
            new CustomType("aa", 1),
            new CustomType("aa", 3)
        ).collect(toList());
        env.fromCollection(data3)
                .distinct("aName", "aNumber")
                .print();

    }

    public static class CustomType {
        public String aName;
        public int aNumber;
        // [...]


        public CustomType() {
        }

        public CustomType(String aName, int aNumber) {
            this.aName = aName;
            this.aNumber = aNumber;
        }
    }


}
