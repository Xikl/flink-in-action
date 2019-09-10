package org.ximo.finkinaction.scala.datastream.timewindow

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * 滑动窗口
  *
  * @author xikl
  * @date 2019/9/10
  */
object SlidingEventTimeWindowsApp extends App {

  val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  val input: DataStream[String] = environment.socketTextStream("localhost", 9999)

  //   * This is a shortcut for either `.window(SlidingEventTimeWindows.of(size))` or
  //   * `.window(SlidingProcessingTimeWindows.of(size))` depending on the time characteristic
  //   * set using
  input.flatMap(_.split(",") filter(_.nonEmpty))
    .map((_, 1))
    .keyBy(0)
    // 滑动窗口
//    .timeWindow(Time.seconds(10), Time.seconds(5))
    .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5),
      Time.hours(-8)))
    .sum(1)
    .print()
    .setParallelism(1)

  environment.execute()

}
