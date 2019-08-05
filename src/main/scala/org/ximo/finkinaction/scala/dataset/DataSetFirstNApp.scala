package org.ximo.finkinaction.scala.dataset
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{ExecutionEnvironment, _}

/**
  *
  *
  * @author xikl
  * @date 2019/8/5
  */
object DataSetFirstNApp {


  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val info = List(
      (1, "hadoop"),
      (1, "scala"),
      (1, "java"),
      (2, "python"),
      (2, "vue"),
      (3, "flutter"),
      (4, "flink")
    )
    val data = env.fromCollection(info)

    // (1,hadoop)
    //(1,scala)
    //(1,java)
    data.first(3).print()

    // (3,flutter)
    //(1,hadoop)
    //(1,scala)
    //(4,flink)
    //(2,python)
    //(2,vue)
    data groupBy 0 first 2 print()

    //(3,flutter)
    //(1,hadoop)
    //(1,java)
    //(4,flink)
    //(2,python)
    //(2,vue)
    data groupBy 0 sortGroup(1, Order.ASCENDING) first 2 print()

  }

}
