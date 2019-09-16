package org.ximo.finkinaction.scala.datastream.timewindow

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.ximo.finkinaction.scala.datastream.timewindow.AggregateFunctionApp.input

/**
  *
  *
  * @author xikl
  * @date 2019/9/16
  */
object ProcessWindowFunctionApp extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val input = env.socketTextStream("localhost", 9999)

  input.flatMap(_.split(",") filter (_.nonEmpty))
    .map((_, 1L))
    .keyBy(_._1)
    .timeWindow(Time.seconds(10))
    .process(new MyProcessWindowFUnction)


  class MyProcessWindowFUnction extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
    override def process(key: String,
                         context: Context,
                         elements: Iterable[(String, Long)],
                         out: Collector[String]): Unit = {
      var count = 0L
      // 可以进行排序
      for (elem <- elements) {
        count += 1
      }
      out.collect(s"window ${context.window} count: $count")
    }
  }







}
