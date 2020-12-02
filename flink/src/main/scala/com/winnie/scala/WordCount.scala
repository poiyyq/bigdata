package com.winnie.scala

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val dataSet = env.readTextFile("/Users/admin/code/bigdata/flink/src/main/resources/worldcount.txt")
    val value = dataSet.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    value.print()
  }
}
