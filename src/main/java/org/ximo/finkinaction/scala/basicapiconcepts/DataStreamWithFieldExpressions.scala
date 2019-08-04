package org.ximo.finkinaction.scala.basicapiconcepts

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._

/**
  * java中tuple KeyBy("f0") 他是从0开始的 scala 则是 _1 从1开始这里要注意一下
  *
  * @author xikl
  * @date 2019/8/4
  */
object DataStreamWithFieldExpressions {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost", 9999)

    val counts = text.flatMap { _.toLowerCase.split(",") filter { _.nonEmpty } }
      .map { WordCount(_, 1) }
      .keyBy("word")
      .timeWindow(Time.seconds(5))
      .sum("count")
      .setParallelism(1)

    counts.print()

    env.execute("Window Stream WordCount")
  }

  // case class 牛逼
  case class WordCount(word: String, count: Int) {

  }



}
