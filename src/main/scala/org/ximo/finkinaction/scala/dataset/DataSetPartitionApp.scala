package org.ximo.finkinaction.scala.dataset

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.ximo.finkinaction.scala.constants.CommonData
/**
  *
  *
  * @author xikl
  * @date 2019/8/27
  */
object DataSetPartitionApp {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = env.fromCollection(CommonData.input3)

    // 不一样的结果
    """
      |仔细分析就会发现
      |采用aggregate的方式 是对所有的数据进行操作
      |而sum和min则仅仅只是对上一个输出的数据进行操作
      |
    """.stripMargin
    data
      // (14,vue,3)
      .aggregate(Aggregations.SUM, 0)
      .andMax(1)
      .and(Aggregations.MIN, 2)
      // (14,flink,89)
//        .sum(0)
      .print()

    /**
      * 3
      * 2
      * 2
      * 分为三组数据
      */
    data
      .mapPartition(res => List(res.length)).print()

    /**
      * (1,java,100)
      * (1,scala,4)
      * (1,hadoop,3)
      * (2,python,1002)
      * (2,vue,200)
      * (3,flutter,90)
      * (4,flink,89)
      *
      */
    data
      .sortPartition(0, Order.ASCENDING)
      .sortPartition(2, Order.DESCENDING)
        .print()

    println(")_________")
    data
      .partitionByHash(0)
      .mapPartition(res => {
        println("_____________________________________" + Thread.currentThread().getId)
        println(res)
        res
      })
//      .setParallelism(3)
      .print()
  }



}
