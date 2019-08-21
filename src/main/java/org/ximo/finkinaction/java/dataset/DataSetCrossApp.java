package org.ximo.finkinaction.java.dataset;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author xikl
 * @date 2019/8/22
 */
public class DataSetCrossApp {

    public static void main(String[] args) throws Exception {
        List<String> info1 = Stream.of("java", "python").collect(Collectors.toList());
        List<Integer> info2 = Stream.of(1, 3, 4).collect(Collectors.toList());

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final DataSource<String> data1 = env.fromCollection(info1);
        final DataSource<Integer> data2 = env.fromCollection(info2);
        data1.cross(data2).print();
        // 你可以建立一个对象 然后这里with进行选择你想要的字段
//                .with()
//        data1.crossWithHuge()
//        data1.crossWithTiny()


    }
}
