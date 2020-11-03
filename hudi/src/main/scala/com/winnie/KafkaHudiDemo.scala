package com.winnie

import java.time.LocalDateTime

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.streaming.StreamingQueryListener

object KafkaHudiDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .set("spark.default.parallelism","9")
      .set("spark.sql.shuffle.partitions","9")
    // 创建spark客户端
    val sparkSession = SparkSession.builder()
      .config(sparkConf)
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    // 添加监听器，每次处理完一批数据都进行日志输出，如输出offset位置信息等
    sparkSession.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
        println(s"Query started: ${event.id}")
      }

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        println(s"Query made progress: ${event.progress}")
      }

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
        println(s"Query terminated: ${event.id}")
      }
    })

    // 定义kafka流
    val dataStreamReader = sparkSession.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "10.2.10.186:9092")
      .option("subscribe", "testTopic")
      .option("startingOffsets", "latest")
      .option("group.id","yyq-test-group")
      .option("maxOffsetPerTrigger", 100000)
      .option("failOnDataLoss", false)

    // 加载流数据
    val df = dataStreamReader.load()
      .selectExpr(
        "topic as kafka_topic",
        "CAST(partition AS STRING) kafka_partition",
        "CAST(timestamp AS STRING) kafka_timestamp",
        "CAST(offset AS STRING) kafka_offset",
        "CAST(key AS STRING) kafka_key",
        "CAST(value AS STRING) kafka_value",
        "current_timestamp() current_time"
      )
      .selectExpr(
        "kafka_topic",
        "concat(kafka_partition,'-',kafka_offset) kafka_partition_offset",
        "kafka_offset",
        "kafka_timestamp",
        "kafka_key",
        "kafka_value",
        "substr(current_time,1,10) partition_date"
      )

    // 创建并启动query
    val query = df.writeStream.queryName("demo")
      .foreachBatch { (batchDF: DataFrame, _: Long) => {
        batchDF.persist()
        println(LocalDateTime.now() + "start writing COW table")

        batchDF.write.format("org.apache.hudi")
          .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, "COPY_ON_WRITE")
          .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "kafka_partition_offset") // 主键
          .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "kafka_timestamp")
          .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "partition_date") // 分区
          .option(HoodieWriteConfig.TABLE_NAME, "copy_on_write_table")
          .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY, true)
          .option(DataSourceWriteOptions.HIVE_URL_OPT_KEY, "jdbc:hive2://testcdh001:10000/default")
          .mode(SaveMode.Append)
          .save("/Volumes/disk/yyq/tmp/kafka-hudi/COPY_ON_WRITE")
        println(LocalDateTime.now() + "start writing mor table")

        batchDF.write.format("org.apache.hudi")
          .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, "MERGE_ON_READ")
          .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, "COPY_ON_WRITE")
          .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "kafka_timestamp")
          .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "kafka_partition_offset")
          .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "partition_date")
          .option(HoodieWriteConfig.TABLE_NAME, "merge_on_read_table")
          .option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING_OPT_KEY, true)
          .option(DataSourceWriteOptions.HIVE_URL_OPT_KEY, "jdbc:hive2://testcdh001:10000/")
          .mode(SaveMode.Append)
          .save("/Volumes/disk/yyq/tmp/kafka-hudi/MERGE_ON_READ")

        println(LocalDateTime.now() + "finish")
        batchDF.unpersist()
      }
      }
      .option("checkpointLocation", "/Volumes/disk/yyq/tmp/kafka-hudi/checkpoint/")
      .start()

    query.awaitTermination()
  }
}
