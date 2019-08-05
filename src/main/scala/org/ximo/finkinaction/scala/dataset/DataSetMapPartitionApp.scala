package org.ximo.finkinaction.scala.dataset

import org.apache.flink.api.common.functions.MapPartitionFunction
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.util.Collector

/**
  *
  *
  * @author xikl
  * @date 2019/8/5
  */
object DataSetMapPartitionApp {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
//    mapPartition1(env)
    mapPartition2(env)
  }


  def mapPartition1(env: ExecutionEnvironment): Unit = {
    env.fromCollection(1 to 100)
      .setParallelism(3)
      .mapPartition((values: Iterator[Int], collector: Collector[Int]) => {
        var c = 0
        for (s <- values) {
          c += 1
        }
        collector.collect(c)
      }).print()
  }

  def mapPartition2(env: ExecutionEnvironment): Unit = {
    env.fromCollection(1 to 100)
      .setParallelism(3)
      .mapPartition(values => List(values.length))
      .print()
  }

  def mapPartition3(env: ExecutionEnvironment): Unit = {
    val partitionMapper: MapPartitionFunction[Int, Int] = (values: Iterator[Int], collector: Collector[Int]) => {
      var c = 0
      for (s <- values) {
        c += 1
      }
      collector.collect(c)
    }

    env.fromCollection(1 to 100)
      .setParallelism(3)
      .mapPartition(partitionMapper)
      .print()
  }


}
