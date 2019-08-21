package org.ximo.finkinaction.scala.dataset

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

/**
  *
  *
  * @author xikl
  * @date 2019/8/21
  */
object DataSetCrossApp {

  /**
    * (java,20)
    * (java,30)
    * (java,40)
    * (python,20)
    * (python,30)
    * (python,40)
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val info1 = List("java", "python")
    val info2 = List(20, 30, 40)
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)

    data1.cross(data2)
      .print()
//    data1.crossWithHuge(data2)
//    data2.crossWithTiny(data1)
  }


}
