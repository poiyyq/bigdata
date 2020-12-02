package com.winnie.scala

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StringDeserializer
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.util.Random

// 温度传感器读数样例类
case class SensorReading(id:String, timestamp:Long, temperature:Double)

object SourceTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    // 1. 从集合获取数据源
//    var stream1 = env.fromCollection(List(
//      SensorReading("1", 1547718199, 35.80018327300259),
//      SensorReading("1", 1547718199, 35.80018327300259),
//      SensorReading("1", 1547718199, 35.80018327300259),
//      SensorReading("1", 1547718199, 35.80018327300259)
//    ));
//
//    stream1.print("stream1").setParallelism(2)

    // 2. 从文件读取
//    val stream2 = env.readTextFile("/Users/admin/code/bigdata/flink/src/main/resources/sensor.txt", "UTF-8")
//    stream2.print("Sensor").setParallelism(4)

    // 3. 从自定义元素中读取
//    val stream3 = env.fromElements(1, 3.1415, "abc", List(1, 2, 3))
//    stream3.print().setParallelism(4)

    // 4. 从kafka中消费数据
//    val topic = "sensor"
//    val props:Properties = new Properties()
//    props.put("bootstrap.servers", "10.14.8.163:9092")
//    props.put("zookeeper.connect","10.14.8.163:2181")
//    props.put("group.id","yyq-sensor")
//    val kafkaSource:FlinkKafkaConsumer011[String] = new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), props)
//    val stream4 = env.addSource(kafkaSource)
//    stream4.flatMap(_.split(" "))
//        .map((_,1))
//        .keyBy(0)
//        .sum(1)
//        .print()

    // 5. 自定义Source
//    val stream5 = env.addSource(new SensorSource())
//    stream5.print()

    env.execute("job")
  }
}


class SensorSource extends SourceFunction[SensorReading]{
  // 执行状态
  var running:Boolean = true

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    // 初始化10个数据,  ("sensorId", 25.322312312)
    val random = new Random()
    // 随机生成数据
    var currTemp = 1.to(10).map(
      t => ("sensor_"+t, random.nextGaussian())
    )
    // 数据收集
    while(running){
      currTemp.map(
        t => sourceContext.collect(SensorReading(t._1, System.currentTimeMillis(), t._2))
      )
      // 短暂睡眠
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}