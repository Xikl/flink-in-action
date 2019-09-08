package org.ximo.finkinaction.scala.datastream

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, SplitStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
/**
  *
  *
  * @author xikl
  * @date 2019/9/8
  */
object DataStreamSideOutputsApp extends App {

  val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  val input = environment.fromCollection(1 to 100)

  val outputTag = OutputTag[String]("side-ouput")

//  input.process(new ProcessFunction[Int, Int] {
//    override def processElement(value: Int,
//                                ctx: ProcessFunction[Int, Int]#Context,
//                                out: Collector[Int]): Unit = {
//      out.collect(value)
//      ctx.output(outputTag, s"side out - ${String.valueOf(value)}")
//    }
//  })

  /**
    * 旁路输出
    * out 是接着调用链进行操作 如 可以接 map  filter
    * ctx 是放在上线文 通过getTag获得另一个输出
    *
    * 代替 split 和 select
    *
    * @see ProcessFunction
    *      KeyedProcessFunction
    *      CoProcessFunction
    *      KeyedCoProcessFunction
    *      ProcessWindowFunction
    *      ProcessAllWindowFunction
     */
  val mainDataStream = input
    .process((value: Int,
              ctx: ProcessFunction[Int, Int]#Context,
              out: Collector[Int]) => {
      // emit data to regular output

      if (value % 2 == 1) {
        out.collect(value)
      } else {
        // emit data to side output
        ctx.output(outputTag, "sideout-" + String.valueOf(value))
      }
    })

  val sideOutputStream: DataStream[String] = mainDataStream.getSideOutput(outputTag)

  sideOutputStream.print().setParallelism(1)

  print("____________________________")

  mainDataStream.print().setParallelism(1)

  environment.execute()



}
