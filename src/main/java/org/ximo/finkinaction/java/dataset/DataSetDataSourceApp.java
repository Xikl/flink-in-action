package org.ximo.finkinaction.java.dataset;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.ximo.finkinaction.java.Person;

/**
 * @author xikl
 * @date 2019/8/4
 */
public class DataSetDataSourceApp {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                // read text file from local files system
        DataSet<String> localLines = env.readTextFile("file:///path/to/my/textfile");

                // read text file from a HDFS running at nnHost:nnPort
        DataSet<String> hdfsLines = env.readTextFile("hdfs://nnHost:nnPort/path/to/my/textfile");

                // read a CSV file with three fields
        DataSet<Tuple3<Integer, String, Double>> csvInput = env.readCsvFile("hdfs:///the/CSV/file")
                .ignoreFirstLine()
                .types(Integer.class, String.class, Double.class);

        // read a CSV file with five fields, taking only two of them
        DataSet<Tuple2<String, Double>> csvInput2 = env.readCsvFile("hdfs:///the/CSV/file")
                .includeFields("10010") // take the first and the fourth field
                .types(String.class, Double.class);

        // read a CSV file with three fields into a POJO (Person.class) with corresponding fields
        DataSet<Person> csvInput4 = env.readCsvFile("hdfs:///the/CSV/file")
                .pojoType(Person.class, "name", "age", "zipcode");

        // read a file from the specified path of type SequenceFileInputFormat
//        DataSet<Tuple2<IntWritable, Text>> tuples =
//                env.createInput(HadoopInputs.readSequenceFile(IntWritable.class, Text.class, "hdfs://nnHost:nnPort/path/to/file"));

        // creates a set from some given elements
        DataSet<String> value = env.fromElements("Foo", "bar", "foobar", "fubar");

        // generate a number sequence
        DataSet<Long> numbers = env.generateSequence(1, 10000000);

        // Read data from a relational database using the JDBC input format
//        DataSet<Tuple2<String, Integer> dbData =
//                env.createInput(
//                        JDBCInputFormat.buildJDBCInputFormat()
//                                .setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
//                                .setDBUrl("jdbc:derby:memory:persons")
//                                .setQuery("select name, age from persons")
//                                .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO))
//                                .finish()
//                );

        // Note: Flink's program compiler needs to infer the data types of the data items which are returned
        // by an InputFormat. If this information cannot be automatically inferred, it is necessary to
        // manually provide the type information as shown in the examples above.

        // 递归读取
        Configuration config = new Configuration();
        // 官网中说明需要添加该参数
        config.setBoolean("recursive.file.enumeration", true);
        env.readTextFile("")
                .withParameters(config)
                .print();

        // readTextFile 可以支持特定的压缩文件
        // https://ci.apache.org/projects/flink/flink-docs-release-1.8/dev/batch/#read-compressed-files

    }


}
