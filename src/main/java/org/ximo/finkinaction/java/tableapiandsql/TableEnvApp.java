package org.ximo.finkinaction.java.tableapiandsql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.types.Row;
import org.ximo.finkinaction.java.Person;
import org.ximo.finkinaction.java.constants.CommonData;

/**
 * @author xikl
 * @date 2019/9/9
 */
public class TableEnvApp {

    /**
     *
     * @see BatchTableEnvironment#registerDataSet(String, DataSet, String) 最后一个字段是列名，用逗号分隔
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final BatchTableEnvironment batchTableEnvironment = BatchTableEnvironment.create(env);

        final DataSource<Person> personDataSource = env.fromCollection(CommonData.PERSONA_LIST);

        batchTableEnvironment.registerDataSet("test1", personDataSource);

        //language=sql
        String sql = "select name, count(1) from test1 group by name";
        final Table table = batchTableEnvironment.sqlQuery(sql);
        table.printSchema();

        batchTableEnvironment.toDataSet(table, Row.class).print();

        // 读取一个文件操作
        final CsvTableSink csvTableSink = new CsvTableSink("", ",");
        String[] fieldNames = {"a", "b", "c"};
        TypeInformation[] fieldTypes = {Types.INT, Types.STRING, Types.LONG};
        csvTableSink.configure(fieldNames, fieldTypes);

        batchTableEnvironment.registerTableSink("testTable", csvTableSink);

        batchTableEnvironment.sqlUpdate(
                "INSERT INTO RevenueFrance " +
                        "SELECT cID, cName, SUM(revenue) AS revSum " +
                        "FROM Orders " +
                        "WHERE cCountry = 'FRANCE' " +
                        "GROUP BY cID, cName"
        );

        Table inputTable = batchTableEnvironment.sqlQuery("select name, age from test1");
        // 直接写入testTable中
        inputTable.insertInto("testTable");

        // from... 操作

        // batchTableEnvironment.fromDataSet()
        // 同样 dataStream也有类似的操作
        // fromDataStream


        // 一些常用的操作
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(streamEnv);

        // https://ci.apache.org/projects/flink/flink-docs-release-1.9/zh/dev/table/common.html#convert-a-table-into-a-datastream
        // 可回收流，就是可修改的
        // DataStream<Row> dsRow = streamTableEnv.toAppendStream(table, Row.class);
    }

}
