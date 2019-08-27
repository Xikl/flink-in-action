package org.ximo.finkinaction.java.dataset;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.ximo.finkinaction.java.constants.CommonData;

/**
 * @author xikl
 * @date 2019/8/28
 */
public class DataSetCountApp {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final DataSource<Tuple2<Integer, String>> data = env.fromCollection(CommonData.TUPLE2_LIST);
        data.map(new RichMapFunction<Tuple2<Integer, String>, Long>() {
            LongCounter count = new LongCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                getRuntimeContext().addAccumulator("counter", count);
            }

            @Override
            public Long map(Tuple2<Integer, String> value) throws Exception {
                count.add(1);
                return count.getLocalValue();
            }
        }).writeAsText("test.txt", WriteMode.OVERWRITE);

        final JobExecutionResult result = env.execute("count-app");
        // 获得count的结果
        final Long counter = result.<Long>getAccumulatorResult("counter");
        System.out.println("count:" + counter);
    }


}
