package com.winnie.scala.es

import java.util
import java.util.Properties

import com.winnie.scala.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests;

object EsSink{
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 数据源配置
    val props = new Properties()
    props.put("bootstrap.servers", "10.14.8.163:9092")
    props.put("group.id","test1")
    val kafkaSource = env.addSource(new FlinkKafkaConsumer011[String]("topic1", new SimpleStringSchema(), props))
    val transformStream = kafkaSource.map(
      t => {
        val words = t.split(",")
        SensorReading(words(0).trim,words(1).trim.toLong,words(2).trim.toDouble)
      }
    )
    // 数据发送配置
    val httpports = new util.ArrayList[HttpHost]()
    httpports.add(new HttpHost("10.14.8.163",9200))
    val esSink = new ElasticsearchSink.Builder[SensorReading](httpports, new ElasticsearchSinkFunction[SensorReading] {
      override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        println(s"接受数据${t}")
        val map = new util.HashMap[String, Any]()
        map.put("id", t.id)
        map.put("temperature", t.temperature)
        map.put("timestamp", t.timestamp)
        val request = Requests.indexRequest()
          .index("sensor")
          .`type`("sensordata")
          .source(map)
        requestIndexer.add(request)
        println("数据已传输完毕")
      }
    })

    // 输出
    transformStream.addSink(esSink.build())

    env.execute("es sink")
  }
}
