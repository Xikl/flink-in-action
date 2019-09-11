package org.ximo.finkinaction.scala.datastream.timewindow

import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  *
  *
  * @author xikl
  * @date 2019/9/12
  */
object AggregateFunctionApp  extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val input = env.socketTextStream("localhost", 9999)

  input.flatMap(StringUtils.split(_, ",") filter(StringUtils.isNoneEmpty(_)))
    .map((_, 1))
    .keyBy(0)
//    .window(TumblingEventTimeWindows.of(Time.seconds(5)))
    .timeWindow(Time.seconds(5))
  // todo
//    .aggregate(new AverageAggregate(), )


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
}
