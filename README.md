# Flink in action GO!
## BatchJob
```java
public static void main(String[] args) throws Exception {
    // set up the batch execution environment
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    /*
     * Here, you can start creating your execution plan for Flink.
     *
     * Start with getting some data from the environment, like
     * 	env.readTextFile(textPath);
     *
     * then, transform the resulting DataSet<String> using operations
     * like
     * 	.filter()
     * 	.flatMap()
     * 	.join()
     * 	.coGroup()
     *
     * and many more.
     * Have a look at the programming guide for the Java API:
     *
     * http://flink.apache.org/docs/latest/apis/batch/index.html
     *
     * and the examples
     *
     * http://flink.apache.org/docs/latest/apis/batch/examples.html
     *
     */

    // execute program
    env.execute("Flink Batch Java API Skeleton");
}
```
## StreamingJob
官网给出的例子 利用控制台进行socket输入
```jshelllanguage
nc -lk 9999
```
windows下需要下载 [netcat](https://eternallybored.org/misc/netcat/),然后输入
```jshelllanguage
nc -l -p 9999
```
```java
public static void main(String[] args) throws Exception {
    // set up the streaming execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    /*
     * Here, you can start creating your execution plan for Flink.
     *
     * Start with getting some data from the environment, like
     * 	env.readTextFile(textPath);
     *
     * then, transform the resulting DataStream<String> using operations
     * like
     * 	.filter()
     * 	.flatMap()
     * 	.join()
     * 	.coGroup()
     *
     * and many more.
     * Have a look at the programming guide for the Java API:
     *
     * http://flink.apache.org/docs/latest/apis/streaming/index.html
     *
     */

    // execute program
    env.execute("Flink Streaming Java API Skeleton");
}
```
## join
```
OPTIMIZER_CHOOSES：相当于不提供任何提示，将选择留给系统。

BROADCAST_HASH_FIRST：广播第一个输入并从中构建哈希表，由第二个输入探测。如果第一个输入非常小，这是一个很好的策略。

BROADCAST_HASH_SECOND：广播第二个输入并从中构建哈希表，由第一个输入探测。如果第二个输入非常小，这是一个很好的策略。

REPARTITION_HASH_FIRST：系统分区（shuffle）每个输入（除非输入已经分区）并从第一个输入构建哈希表。如果第一个输入小于第二个输入，则此策略很好，但两个输入仍然很大。 注意：这是系统使用的默认回退策略，如果不能进行大小估计，并且不能重新使用预先存在的分区和排序顺序。

REPARTITION_HASH_SECOND：系统分区（shuffle）每个输入（除非输入已经分区）并从第二个输入构建哈希表。如果第二个输入小于第一个输入，则此策略很好，但两个输入仍然很大。

REPARTITION_SORT_MERGE：系统对每个输入进行分区（shuffle）（除非输入已经分区）并对每个输入进行排序（除非它已经排序）。输入通过已排序输入的流合并来连接。如果已经对一个或两个输入进行了排序，则此策略很好。
```