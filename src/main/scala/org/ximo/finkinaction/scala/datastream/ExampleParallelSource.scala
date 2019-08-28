package org.ximo.finkinaction.scala.datastream

import java.util.concurrent.TimeUnit

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  *
  *
  * @author xikl
  * @date 2019/8/29
  */
object ExampleParallelSource extends ParallelSourceFunction[Long] {
  var count = 1L

  var running = true


  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while (running && count < 1000) {
      // this synchronized block ensures that state checkpointing,
      // internal state updates and emission of elements are an atomic operation
      ctx.collect(count)
      count += 1
      // 取消
      if (count > 4) {
        cancel()
      }
      TimeUnit.SECONDS.sleep(2)
    }

  }

  override def cancel(): Unit = {
    running = false
  }

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source = ExampleParallelSource
    val data = env.addSource(source).setParallelism(3)

    data.print()

    env.execute()
  }
}

