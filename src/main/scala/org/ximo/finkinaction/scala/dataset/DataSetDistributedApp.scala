package org.ximo.finkinaction.scala.dataset

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration

/**
  * 分布式缓存 把文件都放在了远端
  *
  * @author xikl
  * @date 2019/8/28
  */
object DataSetDistributedApp extends App {

  /**
    * __________
    * 1
    * 2
    * 3
    * 4
    * __________
    * 4
    * 5
    * 5
    *
    */
  val env = ExecutionEnvironment.getExecutionEnvironment
  env.registerCachedFile("./test.txt", "test-txt")

  val input: DataSet[String] = env.fromElements("java", "spark", "scala")

  val result = input.map(new RichMapFunction[String, Int] {


    override def open(parameters: Configuration): Unit = {
      val file = getRuntimeContext.getDistributedCache.getFile("test-txt")
      val lines = FileUtils.readLines(file)
      println("__________")
      lines.forEach(println(_))
      println("__________")
    }

    override def map(value: String): Int = {
      value.length
    }
  })
  result.print()




}
