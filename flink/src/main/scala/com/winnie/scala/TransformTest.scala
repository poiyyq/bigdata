package com.winnie.scala

import org.apache.flink.streaming.api.scala._

object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val streamFromFile = env.readTextFile("/Users/admin/code/bigdata/flink/src/main/resources/sensor.txt")

    val dataStream: DataStream[SensorReading] = streamFromFile.map(data => {
      val dataStrArray = data.split(",")
      SensorReading(dataStrArray(0).trim, dataStrArray(1).trim.toLong, dataStrArray(2).trim.toDouble)
    })

    dataStream.keyBy(_.id).reduce(
      (x,y) => SensorReading(x.id,x.timestamp,y.temperature)
    )

    // 2. 多流算子转换
    val splitStream = dataStream.split(data => {
      if (data.temperature > 30) Seq("high") else Seq("low")
    })

    val highStream = splitStream.select("high")
    val lowStream = splitStream.select("low")
    val allStream = splitStream.select("high","low")



    env.execute("transform test")
  }
}
