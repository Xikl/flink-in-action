package org.ximo.finkinaction.java.datastream;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author xikl
 * @date 2019/8/29
 */
public class ExampleRichParallelDataSource implements ParallelSourceFunction<Long> {

    private Long count = 1L;

    private volatile boolean running = true;

    @Override
    public void run(SourceContext<Long> ctx) throws InterruptedException {
        while (running) {
            // this synchronized block ensures that state checkpointing,
            // internal state updates and emission of elements are an atomic operation
            ctx.collect(count);
            count++;
            TimeUnit.SECONDS.sleep(2);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ExampleRichParallelDataSource exampleRichParallelDataSource = new ExampleRichParallelDataSource();

        final DataStreamSource<Long> longDataStreamSource =
                env.addSource(exampleRichParallelDataSource).setParallelism(2);
        longDataStreamSource.print();

        // java下的 execute之后的代码是不可到达的
        // 这个类并不能取消 fixme
        CompletableFuture.runAsync(() -> {
            try {
                TimeUnit.SECONDS.sleep(8);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            exampleRichParallelDataSource.cancel();
        });

        env.execute();



    }

}
