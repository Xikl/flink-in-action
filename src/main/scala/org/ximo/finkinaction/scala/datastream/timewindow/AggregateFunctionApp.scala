package org.ximo.finkinaction.scala.datastream.timewindow

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  *
  *
  * @author xikl
  * @date 2019/9/12
  */
object AggregateFunctionApp extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val input = env.socketTextStream("localhost", 9999)

  input.flatMap(_.split(",") filter (_.nonEmpty))
    .map((_, 1L))
    .keyBy(0)
    .timeWindow(Time.seconds(10), Time.seconds(5))
    .aggregate(new AverageAggregate())
    .print()
    .setParallelism(1)


  env.execute()

  class AverageAggregate extends AggregateFunction[(String, Long), (Long, Long), Double] {
    override def createAccumulator(): (Long, Long) = {
      (0L, 0L)
    }

    override def add(value: (String, Long), accumulator: (Long, Long)): (Long, Long) = {
      (value._2 + accumulator._1, accumulator._2 + 1)
    }

    override def getResult(accumulator: (Long, Long)): Double = {
      accumulator._1 / accumulator._2
    }

    override def merge(a: (Long, Long), b: (Long, Long)): (Long, Long) = {
      (a._1 + b._1, a._2 + b._2)
    }
  }

  class MyProcessWindowFunction extends ProcessWindowFunction[Double, (String, Double), String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[Double], out: Collector[(String, Double)]): Unit = {
      val average = elements.iterator.next()
      out.collect((key, average))
    }
  }


}
