package com.winnie.scala.kafka

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

object KafkaSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val props = new Properties()
    props.put("bootstrap.servers","10.14.8.163:9092")
    // 1. 从topic1消费数据数据
    val sourceStream = env.addSource(new FlinkKafkaConsumer011[String]("topic1", new SimpleStringSchema(), props))

    // 2. 转换
    val transformStream = sourceStream.map((_+"hello kafka"))

    // 3. 向topic2产生数据
    transformStream.addSink(new FlinkKafkaProducer011[String]("10.14.8.163:9092", "topic2", new SimpleStringSchema()))
    transformStream.print()

    env.execute("kafka sink")
  }
}
