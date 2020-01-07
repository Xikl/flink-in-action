package org.ximo.flinkinaction.scala.dataset

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.junit.Test

/**
  *
  *
  * @author xikl
  * @date 2019/12/23
  */
class DataStreamTest {

  @Test
  def testWordCount(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost", 9999)

    val counts = text.flatMap {
      _.toLowerCase.split("\\W+") filter {
        _.nonEmpty
      }
    }
      .map {
        (_, 1)
      }
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)


    counts.print().setParallelism(1)

    env.execute("Window Stream WordCount")
  }

  @Test
  def testEventTime(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.socketTextStream("localhost", 9999)
      .flatMap(_.toLowerCase().split(" ") filter (_.nonEmpty))
      .map((_, 1))
      .assignAscendingTimestamps(_ => System.currentTimeMillis())
      .keyBy(0)
      .timeWindow(Time.seconds(10))
      .sum(0)
  }

  @Test
  def testAssignTimestampsAndWatermarks(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.socketTextStream("localhost", 9999)
      .flatMap(_.toLowerCase().split(" ") filter (_.nonEmpty))
      .map((_, 1))
      .assignTimestampsAndWatermarks(new MyWaterMarker(Time.seconds(10)))
      .keyBy(0)
      .timeWindow(Time.seconds(10))
      .sum(0)
  }

  class MyWaterMarker(maxOutOfOrderness: Time) extends BoundedOutOfOrdernessTimestampExtractor[(String, Int)](maxOutOfOrderness: Time) {
    override def extractTimestamp(element: (String, Int)): Long = System.currentTimeMillis()
  }

  @Test
  def testDataStreamOnTimer: Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.generateSequence(1, 1000)
      .map(index => (s"str$index", 1L))
      .keyBy(_._1)
      .process(new CountWithTimeoutFunction)

  }

  case class CountWithTimestamp(key: String, count: Long, lastModified: Long)

  private class CountWithTimeoutFunction extends KeyedProcessFunction[String, (String, Long), (String, Long)] {
    lazy val state: ValueState[CountWithTimestamp] = getRuntimeContext.getState(
      new ValueStateDescriptor[CountWithTimestamp]("myState", classOf[CountWithTimestamp])
    )


    override def processElement(value: (String, Long),
                                ctx: KeyedProcessFunction[String, (String, Long), (String, Long)]#Context,
                                out: Collector[(String, Long)]): Unit = {
      // 模式匹配
      val current = state.value match {
        case null =>
          CountWithTimestamp(value._1, 1, ctx.timestamp)
        case CountWithTimestamp(key, count, _) =>
          CountWithTimestamp(key, count + 1, ctx.timestamp)
      }
      state.update(current)
      ctx.timerService registerEventTimeTimer(current.lastModified + 60000)
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[String, (String, Long), (String, Long)]#OnTimerContext,
                         out: Collector[(String, Long)]): Unit = {
      // 对比if的操作
      state.value match {
        case CountWithTimestamp(key, count, lastModified) if (timestamp == lastModified + 60000) =>
          out.collect((key, count))
        case _ =>
      }
    }

  }

}
