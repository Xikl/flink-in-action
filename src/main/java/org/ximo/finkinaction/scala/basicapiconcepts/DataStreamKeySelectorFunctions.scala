package org.ximo.finkinaction.scala.basicapiconcepts

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  *
  *
  * @author xikl
  * @date 2019/8/4
  */
object DataStreamKeySelectorFunctions {


  case class WC(word: String, count: Int)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost", 9999)

    val counts = text.flatMap { _.toLowerCase.split(",") filter { _.nonEmpty } }
      .map { WordCount(_, 1) }
      // keyBy的用法
      .keyBy(_.word)
      .timeWindow(Time.seconds(5))
      .sum("count")
      .setParallelism(1)

    counts.print()

    env.execute("DataStreamKeySelectorFunctions")
  }

  // case class 牛逼
  case class WordCount(word: String, count: Int)

}
