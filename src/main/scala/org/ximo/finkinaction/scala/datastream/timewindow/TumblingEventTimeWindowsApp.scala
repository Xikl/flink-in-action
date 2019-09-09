package org.ximo.finkinaction.scala.datastream.timewindow

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  *
  *
  * @author xikl
  * @date 2019/9/10
  */
object TumblingEventTimeWindowsApp extends App {


  private val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  private val input: DataStream[String] = environment.socketTextStream("localhost", 9999)

  input.flatMap(_.split(",") filter (_.nonEmpty))
    .map((_, 1))
    .keyBy(0)
    .timeWindow(Time.seconds(5))
    .sum(1)
    .print()
    .setParallelism(1)

  environment.execute()
}
