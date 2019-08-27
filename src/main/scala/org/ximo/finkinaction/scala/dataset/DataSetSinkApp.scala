package org.ximo.finkinaction.scala.dataset

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
  *
  *
  * @author xikl
  * @date 2019/8/28
  */
object DataSetSinkApp extends App {

  val env = ExecutionEnvironment.getExecutionEnvironment

  val data = 1 to 10
  
  val input = env.fromCollection(data)

  // 如果是多个并发 它会是一个文件
  input.writeAsText("./test.txt", WriteMode.OVERWRITE)
      .setParallelism(3)

  // 这一步很关键
  env.execute("sink-test")


}
