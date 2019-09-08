package org.ximo.finkinaction.scala.tableapiandsql

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.types.Row
import org.ximo.finkinaction.scala.constants.CommonData
import org.ximo.finkinaction.scala.domain.Student

/**
  * val bEnv = ExecutionEnvironment.getExecutionEnvironment
  *
  * // 很麻烦
  * val bEnvironment: BatchTableEnvironment = BatchTableEnvironment.create(bEnv)
  *
  *
  * val sEnv = StreamExecutionEnvironment.getExecutionEnvironment
  *
  * val sEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(sEnv)
  *
  * @author xikl
  * @date 2019/9/9
  */
object TableEnvApp extends App {

  val bEnv = ExecutionEnvironment.getExecutionEnvironment

  // 很麻烦
  val bTableEnv: BatchTableEnvironment = BatchTableEnvironment.create(bEnv)

  val input: DataSet[(Int, String, Int)] = bEnv.fromCollection(CommonData.input3)

  val studentInput: DataSet[Student] = input.map(item => new Student(item._1, item._2, item._3))

  val table: Table = bTableEnv.fromDataSet(studentInput)

  val test_table: Unit = bTableEnv.registerDataSet("test_table", studentInput)

  studentInput.print()
  table.printSchema()

  println("____________")
//  val resultTable: Table = bTableEnv.sqlQuery("select * from test_table")
//
//  // 直接为一行来操作
//  bTableEnv.toDataSet[Row](resultTable).print()

  val resultTable2: Table = bTableEnv.sqlQuery("select name, count(1) from test_table group by name")

  bTableEnv.toDataSet[Row](resultTable2).print()

}

