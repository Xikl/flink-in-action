package org.ximo.flinkinaction.scala.dataset

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
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

  @Test
  def testEventTime(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.socketTextStream("localhost", 9999)
      .flatMap(_.toLowerCase().split(" ") filter(_.nonEmpty))
      .map((_, 1))
      .assignAscendingTimestamps(a => System.currentTimeMillis())
      .keyBy(0)
      .timeWindow(Time.seconds(10))
      .sum(0)
  }

  @Test
  def testAssignTimestampsAndWatermarks(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.socketTextStream("localhost", 9999)
      .flatMap(_.toLowerCase().split(" ") filter(_.nonEmpty))
      .map((_, 1))
      .assignTimestampsAndWatermarks(new MyWaterMarker(Time.seconds(10)))
      .keyBy(0)
      .timeWindow(Time.seconds(10))
      .sum(0)
  }

  class MyWaterMarker(maxOutOfOrderness: Time) extends BoundedOutOfOrdernessTimestampExtractor[(String, Int)](maxOutOfOrderness: Time) {
    override def extractTimestamp(element: (String, Int)): Long = System.currentTimeMillis()
  }

}
