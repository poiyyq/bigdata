package com.winnie.scala

import com.sun.deploy.util.ParameterUtil
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
 * 流式处理
 */
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val param = ParameterTool.fromArgs(args)
    var host: String = param.get("host")
    var port: Int = param.getInt("port")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 计算逻辑
    val dataStream = env.socketTextStream(host, port)
    dataStream.flatMap(_.split(" ")).map((_,1)).keyBy(0).sum(1).print()

    env.setParallelism(2)
    env.execute("Stream Word Count")
  }

}
