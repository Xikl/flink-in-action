package org.ximo.finkinaction.scala.datastream.timewindow

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * 聚和函数
  *
  * @author xikl
  * @date 2019/9/10
  */
object ReduceFunctionApp extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val input = env.socketTextStream("localhost", 9999)
  input.flatMap(_.split(",") filter(_.nonEmpty))
    .map((_, 1))
    .keyBy(0)
    .timeWindow(Time.seconds(10), Time.seconds(5))
    .reduce((prev, next) => (prev._1, prev._2 + next._2))
    .print()
    .setParallelism(1)

  env.execute()

}
