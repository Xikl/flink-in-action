package org.ximo.finkinaction.java.dataset;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.ximo.finkinaction.java.constants.CommonData;

/**
 * @author xikl
 * @date 2019/8/28
 */
public class DataSetSinkApp {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final DataSource<Tuple2<Integer, String>> text = env.fromCollection(CommonData.TUPLE2_LIST);

        text.writeAsText("./test.txt", FileSystem.WriteMode.OVERWRITE);

        env.execute("sink-test");
    }

}
