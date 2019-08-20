package org.ximo.finkinaction.java.dataset;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import scala.Int;

import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * @author xikl
 * @date 2019/8/7
 */
public class DataSetJoinApp {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        List<User> userData = Stream.of(
            new User("a", 1),
            new User("b", 1),
            new User("c", 2),
            new User("d", 2),
            new User("e", 4)
        ).collect(toList());
        final DataSource<User> userInput = env.fromCollection(userData);

        List<Store> storeData = Stream.of(
                new Store(new Manager("m1"), 1),
                new Store(new Manager("m1"), 1),
                new Store(new Manager("m2"), 2),
                new Store(new Manager("m2"), 2),
                new Store(new Manager("m4"), 3)
        ).collect(toList());
        final DataSource<Store> storeInput = env.fromCollection(storeData);

        userInput.join(storeInput)
                .where("zip")
                .equalTo("zip")
                // with 可以指定返回列 join的实现
                .with((user, store) -> new Tuple3<>(user.zip, user.name, store.mgr.mName))
                // lambda要指定类型
                .returns(Types.TUPLE(Types.INT, Types.STRING, Types.STRING))
                .print();

        System.out.println("分割线————————————————");

        // void join (IN1 first, IN2 second, Collector<OUT> out)
        userInput.join(storeInput)
                .where("zip")
                .equalTo("zip")
                // flatJoin的实现
                .with((User user, Store store, Collector<Tuple3<Integer, String, String>> col) -> {
                    col.collect(new Tuple3<>(user.zip, user.name, store.mgr.mName));
                })
                .returns(Types.TUPLE(Types.INT, Types.STRING, Types.STRING))
                .print();
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    public static class User {
        public String name;
        public int zip;
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    public static class Store {
        public Manager mgr;
        public int zip;
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    public static class Manager {
        public String mName;

    }
}
