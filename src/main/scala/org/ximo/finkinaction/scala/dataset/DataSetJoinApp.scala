package org.ximo.finkinaction.scala.dataset

import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.util.Collector
import org.ximo.finkinaction.scala.constants.CommonData

/**
  *
  *
  * @author xikl
  * @date 2019/8/21
  */
object DataSetJoinApp {


  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data1 = env.fromCollection(CommonData.input1)
    val data2 = env.fromCollection(CommonData.input2)

//    data1.join(data2, JoinHint.BROADCAST_HASH_FIRST)
//    data1.joinWithHuge(data2)
//    data2.joinWithTiny(data1)

    data1.join(data2)
      .where(0)
      .equalTo(0)
      .apply((left, right) => {
        (left._1, left._2, right._2)
      })

    data1.leftOuterJoin(data2)
      .where(0)
      .equalTo(0) {
        (left, right, collect: Collector[(Int, String, String)]) => {
//          if (right == null) {
//            collect.collect(left._1, left._2, null)
//          } else {
//            collect.collect(left._1, left._2, right._2)
//          }
          collect.collect(left._1, left._2, if (right == null) null else right._2)
        }
      }

  }



}
