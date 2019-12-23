package org.ximo.flinkinaction.scala.dataset

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.utils.DataSetUtils
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.junit.{Assert, Test}

/**
 *
 *
 * @author xikl
 * @date 2019/12/9
 */
@Test
class DataSetTest {

  @Test
  def testDataSetTransformations(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.fromElements(
      "Who's there?",
      "I think I hear them. Stand, ho! Who's there?")

    val counts = text.flatMap {
      _.toLowerCase.split(" ") filter {
        _.nonEmpty
      }
    }.map((_, 1))
      .map(s => (s._1.toUpperCase, s._2))
      .filter(!_._1.contains("?"))
      .groupBy(0)
      .sum(1)
      .first(5)
      .sortPartition(0, Order.ASCENDING)
      .sortPartition(1, Order.DESCENDING)

    counts.print()
  }

  @Test
  def testZipWithIndex(): Unit = {
    // 需要导入 import org.apache.flink.api.scala.utils.DataSetUtils 在 当前作用域 才能触发 隐式转换
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    val input: DataSet[String] = env.fromElements("A", "B", "C", "D", "E", "F", "G", "H")
    val zipResult = input.zipWithIndex
    zipResult.print()
  }

  @Test
  def testZipWithUniqueId(): Unit = {
    // 生成唯一ID 可能不是 每次累加1 如 1 3 5 9 34
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    val input: DataSet[String] = env.fromElements("A", "B", "C", "D", "E", "F", "G", "H")

    val result: DataSet[(Long, String)] = input.zipWithUniqueId
    result.print()
  }


}
