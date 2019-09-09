package org.ximo.finkinaction.scala.datastream.timewindow

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  *
  *
  * @author xikl
  * @date 2019/9/10
  */
object StreamTimeCharacteristicApp extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)



}
