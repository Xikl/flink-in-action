package org.ximo.finkinaction.java.constants;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import scala.Int;

import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * @author xikl
 * @date 2019/8/21
 */
public class CommonData {

    public static final List<Tuple3<Integer, String, String>> TUPLE3_LIST =
            Stream.of(
                    new Tuple3<>(1, "Bob", "地址1"),
                    new Tuple3<>(2, "Jon", "地址2"),
                    new Tuple3<>(3, "Json", "地址3"),
                    new Tuple3<>(4, "Flink", "地址4")
            ).collect(toList());

    public static final List<Tuple2<Integer, String>> TUPLE2_LIST =
            Stream.of(
                new Tuple2<>(1, "你好1"),
                new Tuple2<>(1, "你好12"),
                new Tuple2<>(2, "你好21"),
                new Tuple2<>(3, "你好31")
            ).collect(toList());

}
