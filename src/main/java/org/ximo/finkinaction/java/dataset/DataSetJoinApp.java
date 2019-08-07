package org.ximo.finkinaction.java.dataset;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

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
