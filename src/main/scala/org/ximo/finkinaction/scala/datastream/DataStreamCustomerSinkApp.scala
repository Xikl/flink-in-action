package org.ximo.finkinaction.scala.datastream

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.ximo.finkinaction.scala.domain.Student


/**
  *
  *
  * @author xikl
  * @date 2019/9/8
  */
object DataStreamCustomerSinkApp extends App {

  val environment = StreamExecutionEnvironment.getExecutionEnvironment

  val input = environment.socketTextStream("localhost", 9999)

  input.map(line => {
    val infos: Array[String] = line.split(",")
    Student(infos(0), infos(1).toInt)
  }).addSink(new SinkToMysql)


  environment.execute()


}


class SinkToMysql extends RichSinkFunction[Student] {

  var connection: Connection = _

  var pstmt: PreparedStatement = _

  private def getConnection: Connection = {
    Class.forName("com.mysql.jdbc.Driver")
    val url = "jdbc:mysql://localhost:3306/test?useSSL=true"

    val user = "root"

    val pwd = "root"

    val connection = DriverManager.getConnection(url, user, pwd)
    connection
  }

  override def open(parameters: Configuration): Unit = {
    connection = getConnection
    val sql = "insert into t_student(name, age) values(?, ?)"
    pstmt = connection.prepareStatement(sql)

    // 8核12线程 会打印12个open 并行度
    println("open")
  }

  override def invoke(student: Student, context: SinkFunction.Context[_]): Unit = {
    // 赋值
    pstmt.setString(1, student.name)
    pstmt.setInt(2, student.age)

    pstmt.execute()
  }

  override def close(): Unit = {
    // 防止空指针conn
    for (conn <- Option(connection)){
      conn.close()
    }

    for (optionPstmt <- Option(pstmt)){
      optionPstmt.close()
    }
  }

}
