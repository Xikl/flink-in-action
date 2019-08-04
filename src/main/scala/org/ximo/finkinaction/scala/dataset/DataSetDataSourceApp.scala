package org.ximo.finkinaction.scala.dataset

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.ximo.finkinaction.java.Person

/**
  *
  *
  * @author xikl
  * @date 2019/8/4
  */
object DataSetDataSourceApp {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    // read text file from local files system
    // 可以是一个目录/文件夹
    val localLines = env.readTextFile("file:///path/to/my/textfile")

    // read text file from a HDFS running at nnHost:nnPort
    val hdfsLines = env.readTextFile("hdfs://nnHost:nnPort/path/to/my/textfile")

    // read a CSV file with three fields
    // 这里要指定每一列的类型
    // ignoreFirstLine = true
    val csvInput = env.readCsvFile[(Int, String, Double)]("hdfs:///the/CSV/file", ignoreFirstLine = true)

    // read a CSV file with five fields, taking only two of them
    val csvInputWithTuple = env.readCsvFile[(String, Double)](
      "hdfs:///the/CSV/file",
      includedFields = Array(0, 3)) // take the first and the fourth field

    // CSV input can also be used with Case Classes
    case class MyCaseClass(str: String, dbl: Double)
    val csvInputWithCaseClss = env.readCsvFile[MyCaseClass](
      "hdfs:///the/CSV/file",
      includedFields = Array(0, 3)) // take the first and the fourth field

    // read a CSV file with three fields into a POJO (Person) with corresponding fields
    val csvInput3 = env.readCsvFile[Person](
      "hdfs:///the/CSV/file",
      pojoFields = Array("name", "age"),
      ignoreFirstLine = true)

    // create a set from some given elements
    val values = env.fromElements("Foo", "bar", "foobar", "fubar")

    // generate a number sequence
    val numbers = env.generateSequence(1, 10000000)

    // read a file from the specified path of type SequenceFileInputFormat
//    val tuples = env.createInput(HadoopInputs.readSequenceFile(classOf[IntWritable], classOf[Text],
//      "hdfs://nnHost:nnPort/path/to/file"))

    // 递归读取
    val parameters = new Configuration

    parameters.setBoolean("recursive.file.enumeration", true)
    env.readTextFile("")
      .withParameters(parameters)
      .print()

    // readTextFile 可以支持特定的压缩文件
    // https://ci.apache.org/projects/flink/flink-docs-release-1.8/dev/batch/#read-compressed-files



  }

}
