package org.ximo.finkinaction.scala.examples

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.extensions._

/**
  *
  *
  * @author xikl
  * @date 2019/8/4
  */
object BatchWordCountWithScala {

  def main(args: Array[String]): Unit = {
    val environment = ExecutionEnvironment.getExecutionEnvironment
    val input = "input.txt"
    val text = environment.readTextFile(input)
    text.flatMap(_.toLowerCase.split(" "))
      .filter(_.nonEmpty)
//      .map { (_, 1) }
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .print()

    // import org.apache.flink.api.scala.extensions._ 需要import这个东西
    // scala 高级用法 mapWith 、 xxxWith 、 keyingBy等操作
  }

}
