package org.ximo.finkinaction.scala.dataset

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration

/**
  *
  *
  * @author xikl
  * @date 2019/8/28
  */
object DataSetBroadcastVariablesApp extends App {


  val env = ExecutionEnvironment.getExecutionEnvironment

  val toBroadcast = env.fromElements(1, 2, 3)

  val data = env.fromElements("a", "b")

  data.map(new RichMapFunction[String, String] {

    override def open(parameters: Configuration): Unit = {
      // 这里能拿到广播的变量
      val resultList = getRuntimeContext.getBroadcastVariable[Int]("broadcast-data")
      println("_____")
      resultList.forEach(println(_))
      println("_____")
    }

    override def map(value: String): String = {
      value
    }
  }).withBroadcastSet(toBroadcast, "broadcast-data").print()



}
