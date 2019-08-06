package org.ximo.finkinaction.scala.dataset

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

/**
  *
  *
  * @author xikl
  * @date 2019/8/7
  */
object DataSetFlatMapApp {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = List(
      "java,python",
      "scala,java,test",
      "scala,flink,spark"
    )

    env.fromCollection(data)
      .flatMap(_.split(","))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .print()

  }

}
