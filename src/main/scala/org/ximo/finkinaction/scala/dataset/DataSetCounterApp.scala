package org.ximo.finkinaction.scala.dataset

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
/**
  *
  *
  * @author xikl
  * @date 2019/8/28
  */
object DataSetCounterApp extends App {

  val env = ExecutionEnvironment.getExecutionEnvironment

  val data1 = env.fromElements("java", "flink", "spark", "python", "scala")
  val data2 = env.fromElements("java", "flink", "spark", "python", "scala")

//  data1.map(new RichMapFunction[String, Long] {
//    var count = 0
//    override def map(value: String): Long = {
//      count = count + 1
//      println(s"count: $count")
//      count
//    }
//  })
//    //count: 1
//    //count: 1
//    //count: 1
//    //count: 2
//    //count: 2
//    // 设置并行度 就有问题
//    .setParallelism(3)
//    .print()

    // 正确的写法
  data2.map(new RichMapFunction[String, Long] {
    val count = new  LongCounter()

    // 注册一个
    override def open(parameters: Configuration): Unit = {
      getRuntimeContext.addAccumulator("count-app", count)
    }

    // 操作
    override def map(value: String): Long = {
      count.add(1)
      count.getLocalValuePrimitive
    }
  }).setParallelism(3)
    // 这里要sink一下
    .writeAsText("test.txt", WriteMode.OVERWRITE)

  val result: JobExecutionResult = env.execute("counter")
  val num = result.getAccumulatorResult[Long]("count-app")
  println(s"num: $num")

}
