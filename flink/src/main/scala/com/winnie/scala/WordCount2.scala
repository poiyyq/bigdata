package com.winnie.scala

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

/**
 * 词频统计
 * 批处理
 */
object WordCount2 {
  def main(args: Array[String]): Unit = {
    var filepath = "/Users/admin/code/bigdata/flink/src/main/resources/worldcount.txt"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val dataSet = env.readTextFile(filepath, "UTF-8")
    dataSet.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1).print()






//    val value = dataSet.flatMap(_.split(" "))
//      .map((_, 1))
//      .groupBy(0)
//      .sum(1)
//
//    value.print()
  }
}
