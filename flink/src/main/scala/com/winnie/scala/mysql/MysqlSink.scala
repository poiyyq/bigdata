package com.winnie.scala.mysql

import java.sql.{Connection, Driver, DriverManager, PreparedStatement}
import java.util.Properties

import com.winnie.scala.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object MysqlSink {
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

    transformStream.addSink(new MyJdbcSink())

    env.execute("mysql sink")
  }

}

class MyJdbcSink() extends RichSinkFunction[SensorReading]{
  var conn:Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  /**
   * 初始化、创建链接和预编译语句
   * @param parameters
   */
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn = DriverManager.getConnection("jdbc:mysql://10.14.8.163:3306/winnie","root","root")
    insertStmt = conn.prepareStatement("insert into sensor(id,temp) values(?,?)")
    updateStmt = conn.prepareStatement("update sensor set temp = ? where id = ?")
  }

  /**
   * 调用链接执行sql
   * @param t
   * @param context
   */
  override def invoke(t: SensorReading, context: SinkFunction.Context[_]): Unit = {
    updateStmt.setDouble(1, t.temperature)
    updateStmt.setString(2, t.id)
    updateStmt.execute()
    // 如果update没有更新成功，则插入
    if(updateStmt.getUpdateCount == 0){
      insertStmt.setString(1, t.id)
      insertStmt.setDouble(2, t.temperature)
      insertStmt.execute()
    }
  }

  /**
   * 关闭时做清理工作
   */
  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}
