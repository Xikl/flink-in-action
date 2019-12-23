package org.ximo.flinkinaction.scala.dataset

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.junit.Test

/**
  *
  *
  * @author xikl
  * @date 2019/12/23
  */
class DataStreamTest {

  @Test
  def testWordCount() : Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost", 9999)

    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)


    counts.print().setParallelism(1)

    env.execute("Window Stream WordCount")
  }



}
