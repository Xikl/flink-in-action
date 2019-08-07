package org.ximo.finkinaction.scala.dataset

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

/**
  *
  *
  * @author xikl
  * @date 2019/8/7
  */
object DataSetDistinctApp {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.fromCollection(Seq(1, 1, 2, 3, 4, 4, 5, 5))
      .distinct()
      .print()

    env.fromCollection(Seq(1, -1, 2, 3, -4, 4, 5, 5))
        .distinct(x => Math.abs(x))
        .print()

    // (1,2,b)
    //(2,2,b)
    env.fromCollection(
      Seq(
        (1, 2, "b"),
        (2, 2, "b"),
        (1, 2, "b")
      )
    ).distinct(0, 2)
      .print()



    env.fromCollection(Seq(
      CustomType("aa", 2),
      CustomType("aa", 3),
      CustomType("aa", 2),
      CustomType("bb", 3)
    ))
      .distinct("aName", "aNumber")
      .print()
  }

  case class CustomType(aName : String, aNumber : Int)


}
