package org.ximo.finkinaction.java.dataset;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.List;

/**
 * @author xikl
 * @date 2019/8/28
 */
public class DataSetDistributedCacheApp {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final DataSource<String> input = env.fromElements("java", "spark");

        env.registerCachedFile("./test.txt", "test-txt");

        input.map(new RichMapFunction<String, String>() {
            List<String> resultList;

            @Override
            public void open(Configuration parameters) throws Exception {
                final File file = getRuntimeContext().getDistributedCache().getFile("test-txt");
                final List<String> strings = FileUtils.readLines(file);
                resultList = strings;
                System.out.println("_________");
                resultList.forEach(System.out::println);
                System.out.println("_________");
            }

            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).print();


    }


}
