package com.winnie.scala

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object TimeWindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream = env.socketTextStream("localhost", 9999)
    // 最近十秒最低温度
    val timewindowStream = dataStream.map(
      t => {
        val str = t.split(",")
        SensorReading(str(0).trim, str(1).trim().toLong, str(2).trim.toDouble)
      }
    ).map(t=>(t.id,t.temperature))
        .keyBy(_._1)
        .timeWindow(Time.seconds(10), Time.seconds(1))
        .reduce((data1,data2) => (data1._1, data1._2.min(data2._2)))

    dataStream.print("input")
    timewindowStream.print("min")

    env.execute("timewindow test")
  }
}
