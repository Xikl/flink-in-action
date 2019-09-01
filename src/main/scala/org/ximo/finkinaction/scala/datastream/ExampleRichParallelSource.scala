package org.ximo.finkinaction.scala.datastream

import java.util.concurrent.TimeUnit

import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  *无论是java还是flink在执行env.execute 之后的代码除非自己结束，否则都不可到达
  *
  * @author xikl
  * @date 2019/8/29
  */
object ExampleRichParallelSource extends RichParallelSourceFunction[Long]{
  var count = 1L

  var running = true


  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while (running && count < 1000) {
      // this synchronized block ensures that state checkpointing,
      // internal state updates and emission of elements are an atomic operation
      ctx.collect(count)
      count += 1
      // 取消
      TimeUnit.SECONDS.sleep(2)
    }

  }

  override def cancel(): Unit = {
    running = false
  }

  override def open(parameters: Configuration): Unit = super.open(parameters)

  override def close(): Unit = super.close()

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source = ExampleRichParallelSource
    val data = env.addSource(source).setParallelism(3)

    data.print()

    env.execute()

    TimeUnit.SECONDS.sleep(8)

    // 也可以取消 线程 可见性
    source.cancel()
  }
}
