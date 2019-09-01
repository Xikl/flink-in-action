package org.ximo.finkinaction.scala.datastream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  *
  *
  * @author xikl
  * @date 2019/8/29
  */
object DataSourceApp extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val text = env.socketTextStream("localhost", 9999)

  // 设置并行度
  text.print().setParallelism(1)

  env.execute("data-source-test")


}
