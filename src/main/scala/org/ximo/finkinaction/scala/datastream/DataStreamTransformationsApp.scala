package org.ximo.finkinaction.scala.datastream

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, SplitStream, StreamExecutionEnvironment}

import scala.collection.immutable

/**
  *
  *
  * @author xikl
  * @date 2019/9/2
  */
object DataStreamTransformationsApp extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val list: immutable.Seq[Int] = 1 to 10

  val data: DataStream[Int] = env.fromCollection(list)

//  data.map(_ * 2).filter(_ % 2 == 0).print().setParallelism(1)
//
//  // union
//  val data2 = env.fromCollection(11 to 20)
//
//
//  println("----------------")
//  data.union(data2).print().setParallelism(1)

  // todo side outputs
  val split = data.split(
    (num: Int) =>
      (num % 2) match {
        case 0 => List("even")
        case 1 => List("odd")
      }
  )

  split.select("even").print().setParallelism(1)

  env.execute()

}
