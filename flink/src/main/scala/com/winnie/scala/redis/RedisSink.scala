package com.winnie.scala.redis

import java.util.Properties

import com.winnie.scala.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
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

    val config = new FlinkJedisPoolConfig.Builder()
      .setHost("10.14.8.163")
      .setPort(6379)
      .setPassword("123456")
      .build()
    transformStream.addSink(new RedisSink(config, new MyRedisMapper()))
    transformStream.print()
    env.execute("redis sink")
  }
}

class MyRedisMapper extends RedisMapper[SensorReading] {
  /**
   * 定义保存到redis的命令
   * @return
   */
  override def getCommandDescription: RedisCommandDescription = {
    // 把传感器id和温度值保存成哈希表  HSET key field value
    new RedisCommandDescription(RedisCommand.HSET, "sensor_temperature")
  }

  /**
   * 定义保存到redis的value
   * @param data
   * @return
   */
  override def getKeyFromData(data: SensorReading): String = data.id.toString

  override def getValueFromData(data: SensorReading): String = data.toString
}