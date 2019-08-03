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