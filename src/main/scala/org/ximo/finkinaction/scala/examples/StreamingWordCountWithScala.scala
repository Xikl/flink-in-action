package org.ximo.finkinaction.scala.examples

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  *
  *
  * @author xikl
  * @date 2019/8/4
  */
object StreamingWordCountWithScala {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost", 9999)

    val counts = text.flatMap { _.toLowerCase.split(",") filter { _.nonEmpty } }
      .map { (_, 1) }
      .keyBy(0)
      // countWindow
      .timeWindow(Time.seconds(5))
      .sum(1)
      .setParallelism(1)

    counts.print()

    env.execute("Window Stream WordCount")
  }

}
